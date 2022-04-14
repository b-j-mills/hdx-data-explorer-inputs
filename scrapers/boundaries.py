import logging
import re
from os.path import dirname, join

import topojson as tp
from geojson import load, loads
from geopandas import GeoDataFrame, read_file
from hdx.location.country import Country
from pandas import isna
from scrapers.utilities.helper_functions import (
    download_unzip_data,
    find_resource,
    remove_crs,
    replace_json,
    update_mapbox,
)
from shapely.geometry import box

logger = logging.getLogger()


def update_boundaries(
    configuration,
    downloader,
    mapbox_auth,
    temp_folder,
    adm0_json,
    adm1_json,
    visualizations=None,
    countries=None,
):
    exceptions = configuration["boundaries"].get("exceptions")
    if not exceptions:
        exceptions = {}

    if not visualizations:
        visualizations = [key for key in configuration["adm0"]]

    for visualization in visualizations:
        logger.info(f"Editing admin boundaries for {visualization}")

        adm0_countries = configuration["adm0"][visualization]
        adm1_countries = configuration["adm1"][visualization]

        if not countries:  # only update mapbox, not HDX
            update_mapbox(adm0_countries)
            update_mapbox(adm1_countries)

        elif len(countries) == 1 and countries[0].lower() == "all":  # update both
            countries = configuration["adm1"][visualization]
        else:  # update both
            countries = [
                country
                for country in countries
                if country in configuration["adm1"][visualization]
            ]

        for iso in countries:
            logger.info(f"Processing boundaries for {iso}")

            dataset_name = exceptions.get(iso)
            boundary_resource = None
            if dataset_name:
                boundary_resource = find_resource(dataset_name, "SHP")
            if not boundary_resource:
                boundary_resource = find_resource(f"cod-em-{iso.lower()}", "SHP")
            if not boundary_resource:
                boundary_resource = find_resource(f"cod-ab-{iso.lower()}", "SHP")
            if not boundary_resource:
                logger.error(f"Could not find boundary dataset for {iso}")
                continue

            if len(boundary_resource) > 1:
                name_match = [
                    bool(re.match(".*adm.*1.*", r["name"], re.IGNORECASE))
                    for r in boundary_resource
                ]
                boundary_resource = [
                    boundary_resource[i]
                    for i in range(len(boundary_resource))
                    if name_match[i]
                ]

                if len(boundary_resource) != 1:
                    logger.error(f"Could not distinguish between resources for {iso}")
                    continue

            boundary_shp = download_unzip_data(downloader, boundary_resource[0], "shp")
            if not boundary_shp:
                continue

            if len(boundary_shp) > 1:
                name_match = [
                    bool(re.match(".*adm.*1.*", b, re.IGNORECASE)) for b in boundary_shp
                ]
                boundary_shp = [
                    boundary_shp[i] for i in range(len(boundary_shp)) if name_match[i]
                ]

                if len(boundary_shp) != 1:
                    logger.error(
                        f"Could not distinguish between downloaded shapefiles for {iso}"
                    )
                    continue

            boundary_lyr = read_file(boundary_shp[0])

            # calculate fields
            req_fields = ["alpha_3", "ADM0_REF", "ADM0_PCODE", "ADM1_REF", "ADM1_PCODE"]
            boundary_lyr["alpha_3"] = iso.upper()
            boundary_lyr["ADM0_REF"] = Country.get_country_name_from_iso3(iso)
            boundary_lyr["ADM0_PCODE"] = Country.get_iso2_from_iso3(iso)

            fields = boundary_lyr.columns
            pcode_field = None
            name_field = None
            for field in fields:
                if not pcode_field:
                    if field.upper() == "ADM1_PCODE":
                        pcode_field = field
                    if (
                        field.upper()
                        in configuration["shapefile_attribute_mappings"]["pcode"]
                    ):
                        pcode_field = field
                if not name_field:
                    if field.upper() == "ADM1_REF":
                        name_field = field
                    if (
                        field.upper()
                        in configuration["shapefile_attribute_mappings"]["name"]
                    ):
                        name_field = field

            if not pcode_field or not name_field:
                logger.error(f"Could not map pcode or name fields for {iso}")
                continue

            boundary_lyr["ADM1_PCODE"] = boundary_lyr[pcode_field]
            boundary_lyr["ADM1_REF"] = boundary_lyr[name_field]
            boundary_lyr.drop(
                [
                    f
                    for f in boundary_lyr.columns
                    if f not in req_fields and f.lower() != "geometry"
                ],
                axis=1,
                inplace=True,
            )

            boundary_lyr = boundary_lyr.dissolve(by=req_fields, as_index=False)  # dissolve to single features

            boundary_topo = tp.Topology(boundary_lyr)  # simplify
            boundary_topo.toposimplify(
                epsilon=0.01,
                simplify_algorithm="dp",
                simplify_with="simplification",
                inplace=True,
            )
            boundary_lyr = boundary_topo.to_gdf()

            boundary_lyr = boundary_lyr.overlay(adm0_json, how="union")  # overlay with country boundary


            # convert to centroid
            centroid_lyr = GeoDataFrame(boundary_lyr.representative_point())
            centroid_lyr.rename(columns={0: "geometry"}, inplace=True)
            centroid_lyr[req_fields] = centroid_lyr[req_fields]
            centroid_lyr.set_geometry("geometry", inplace=True)
            cent_bound = join(temp_folder, f"{iso.lower()}_centroid.geojson")
            centroid_lyr.to_file(cent_bound, driver="GeoJSON")

            # Merge with existing JSONs
            latest_polbndas = glob(
                join("../Geoprocessing", "latest", adm_level, "*polbnda*.geojson")
            )
            latest_centroids = glob(
                join("../Geoprocessing", "latest", adm_level, "*centroid*.geojson")
            )

            for latest_polbnda in latest_polbndas:
                latest_lyr = read_file(latest_polbnda)
                merged_lyr = latest_lyr.append(new_lyr)
                remove(latest_polbnda)
                merged_lyr.to_file(latest_polbnda, driver="GeoJSON")

            for latest_centroid in latest_centroids:
                latest_lyr = read_file(latest_centroid)
                merged_lyr = latest_lyr.append(new_cent)
                remove(latest_centroid)
                merged_lyr.to_file(latest_centroid, driver="GeoJSON")
                remove_crs(latest_centroid)

            logger.info(f"Done processing {adm_level}")

    # create admin1 lookup for COVID viz yml
    latest_adm1 = glob(
        join("../Geoprocessing", "latest", "boundaries", "hrps*polbnda*.geojson")
    )
    try:
        latest_adm1 = latest_adm1[0]
    except IndexError:
        logger.error(f"Cannot find final admin1 boundary!")
        return

    latest_lyr = read_file(latest_adm1)
    attributes = list()
    for index, row in latest_lyr.iterrows():
        attributes.append(
            {
                "country": row["ADM0_REF"],
                "iso3": row["alpha_3"],
                "pcode": row["ADM1_PCODE"],
                "name": row["ADM1_REF"],
            }
        )
    attributes = sorted(attributes, key=lambda i: (i["country"], i["name"]))

    with open(join("../config", "adm1_attributes.txt"), "w") as f:
        for row in attributes:
            if "," in row["name"]:
                row["name"] = '"' + row["name"] + '"'
            if "'" in row["name"]:
                row["name"] = row["name"].replace("'", "|")
            f.write("- %s\n" % str(row).replace("'", "").replace("|", "'"))

    logger.info("Created admin1 lookup")

    # create regional bb geojson
    logger.info("Updating regional bbox json")

    regional_file = join(
        "../Geoprocessing", "latest", "bbox", "ocha-regions-bbox.geojson"
    )
    with open(regional_file, "r") as f_open:
        regional_lyr = load(f_open)
    latest_adm0 = glob(
        join("../Geoprocessing", "latest", "boundaries", "*polbnda*.geojson")
    )
    try:
        latest_adm0 = latest_adm0[0]
    except IndexError:
        logger.error(f"Cannot find final admin0 boundary!")
        return
    adm0_lyr = read_file(latest_adm0)
    adm0_lyr["region"] = ""
    adm0_lyr["HRPs"] = ""
    adm0_lyr.loc[adm0_lyr["ISO_3"].isin(HRPs), "HRPs"] = "HRPs"
    regional_info = configuration.get("regional")
    read_hdx_metadata(regional_info)
    with Download() as downloader:
        _, iterator = read_tabular(downloader, regional_info)
        for row in iterator:
            adm0_lyr.loc[
                adm0_lyr["ISO_3"] == row[regional_info["iso3"]], "region"
            ] = row[regional_info["region"]]
    adm0_dissolve = adm0_lyr.dissolve(by="region")
    adm0_dissolve_HRPs = adm0_lyr[adm0_lyr["HRPs"] == "HRPs"].dissolve(by="HRPs")
    adm0_dissolve = adm0_dissolve.append(adm0_dissolve_HRPs)
    adm0_dissolve = adm0_dissolve.bounds
    adm0_dissolve["geometry"] = [
        box(l, b, r, t)
        for l, b, r, t in zip(
            adm0_dissolve["minx"],
            adm0_dissolve["miny"],
            adm0_dissolve["maxx"],
            adm0_dissolve["maxy"],
        )
    ]
    adm0_dissolve = GeoDataFrame(adm0_dissolve["geometry"])
    adm0_dissolve["tbl_regcov_2020_ocha_Field3"] = adm0_dissolve.index
    adm0_json = adm0_dissolve.to_json(show_bbox=True, drop_id=True)
    adm0_json = loads(adm0_json)
    regional_lyr["bbox"] = adm0_json["bbox"]
    for i in reversed(range(len(regional_lyr["features"]))):
        region = regional_lyr["features"][i]["properties"][
            "tbl_regcov_2020_ocha_Field3"
        ]
        k = [
            j
            for j in range(len(adm0_json["features"]))
            if adm0_json["features"][j]["properties"]["tbl_regcov_2020_ocha_Field3"]
            == region
        ]
        if len(k) != 1:
            logger.error(f"Cannot find region {region} in new boundaries")
            regional_lyr["features"].remove(regional_lyr["features"][i])
            continue
        regional_lyr["features"][i]["geometry"]["coordinates"] = adm0_json["features"][
            k[0]
        ]["geometry"]["coordinates"]
        regional_lyr["features"][i] = {
            "type": "Feature",
            "properties": regional_lyr["features"][i]["properties"],
            "bbox": adm0_json["features"][k[0]]["bbox"],
            "geometry": regional_lyr["features"][i]["geometry"],
        }
    replace_json(regional_lyr, regional_file)
    logger.info("Updated regional bbox json")
