import logging
import re
from os.path import join
from unicodedata import normalize
import topojson as tp
from geojson import load, loads
from geopandas import GeoDataFrame, read_file
from shapely.geometry import box

from hdx.scraper.utilities.readers import read_hdx_metadata, read_tabular
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from scrapers.utilities.helper_functions import (
    download_unzip_data,
    find_resource,
    remove_crs,
    replace_json,
    drop_fields,
    #update_mapbox,
)

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

    if not countries:
        countries = []
    elif len(countries) == 1 and countries[0].lower() == "all":
        countries = set()
        for viz in visualizations:
            for country in configuration["adm1"][viz]:
                countries.add(country)
        countries = list(countries)

    req_fields = ["alpha_3", "ADM0_REF", "ADM0_PCODE", "ADM1_REF", "ADM1_PCODE"]
    for iso in countries:
        logger.info(f"Processing admin1 boundaries for {iso}")

        country_adm0 = adm0_json.loc[(adm0_json["ISO_3"] == iso) | (adm0_json["COLOR_CODE"] == iso)]
        country_adm0 = country_adm0.dissolve()
        country_adm0 = drop_fields(country_adm0, ["ISO_3"])

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

        boundary_shp = download_unzip_data(downloader, boundary_resource[0], "shp", temp_folder)
        if not boundary_shp:
            continue

        if len(boundary_shp) > 1:
            name_match = [
                bool(re.match(".*adm(in)?1.*", b, re.IGNORECASE)) for b in boundary_shp
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
        if not boundary_lyr.crs:
            boundary_lyr = boundary_lyr.set_crs(crs="EPSG:4326")
        if not boundary_lyr.crs.name == "WGS 84":
            boundary_lyr = boundary_lyr.to_crs(crs="EPSG:4326")

        # calculate fields
        boundary_lyr["alpha_3"] = iso.upper()
        boundary_lyr["ADM0_REF"] = Country.get_country_name_from_iso3(iso)
        boundary_lyr["ADM0_PCODE"] = Country.get_iso2_from_iso3(iso)

        fields = boundary_lyr.columns
        pcode_field = None
        name_field = None
        if "ADM1_PCODE" in fields:
            pcode_field = "ADM1_PCODE"
        if "ADM1_EN" in fields:
            name_field = "ADM1_EN"
        for field in fields:
            if not pcode_field:
                if (
                    field.upper()
                    in configuration["shapefile_attribute_mappings"]["pcode"]
                ):
                    pcode_field = field
            if not name_field:
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
        boundary_lyr = drop_fields(boundary_lyr, req_fields)

        for index, row in boundary_lyr.iterrows():  # remove any special characters from names
            if row["ADM1_REF"]:
                new_name = row["ADM1_REF"].replace("-", " ")
                new_name = normalize("NFKD", new_name).encode("ascii", "ignore").decode("ascii")
                boundary_lyr.loc[index, "ADM1_REF"] = new_name

        na_count = boundary_lyr["ADM1_REF"].isna().sum()
        if na_count > 0:
            logger.warning(f"Found {na_count} null values in {iso} boundary")

        boundary_lyr = boundary_lyr.dissolve(by=req_fields, as_index=False)  # dissolve to single features

        boundary_topo = tp.Topology(boundary_lyr)  # simplify
        boundary_topo = boundary_topo.toposimplify(
            epsilon=0.01,
            simplify_algorithm="dp",
            prevent_oversimplify=True,
        )
        boundary_lyr = boundary_topo.to_gdf(crs="EPSG:4326")

        boundary_union = boundary_lyr.overlay(country_adm0, how="union")  # harmonize with country boundary
        boundary_slivers = boundary_union[["ISO_3", "geometry"]][boundary_union["alpha_3"].isna()]
        boundary_union.dropna(axis=0, inplace=True)
        boundary_slivers = boundary_slivers.explode(ignore_index=True)
        boundary_slivers = boundary_slivers.sjoin(boundary_union, predicate="touches")
        boundary_union = boundary_union.append(boundary_slivers)
        boundary_union = boundary_union.dissolve(by=req_fields, as_index=False)
        boundary_union = drop_fields(boundary_union, req_fields)

        adm1_json = adm1_json[adm1_json["alpha_3"] != iso]
        adm1_json = adm1_json.append(boundary_union)

        logger.info(f"Finished processing admin1 boundaries for {iso}")

    if countries:  # update HDX, admin1 lookups
        for visualization in visualizations:
            logger.info(f"Updating admin1 lookups for {visualization}")
            attributes = list()
            for index, row in adm1_json.iterrows():
                if row["alpha_3"] in configuration["adm1"][visualization]:
                    attributes.append(
                        {
                            "country": row["ADM0_REF"],
                            "iso3": row["alpha_3"],
                            "pcode": row["ADM1_PCODE"],
                            "name": row["ADM1_REF"],
                        }
                    )
            attributes = sorted(attributes, key=lambda i: (i["country"], i["name"]))

            with open(join("saved_outputs", f"adm1-attributes-{visualization}.txt"), "w") as f:
                for row in attributes:
                    if "," in row["name"]:
                        row["name"] = '"' + row["name"] + '"'
                    if "'" in row["name"]:
                        row["name"] = row["name"].replace("'", "|")
                    f.write("- %s\n" % str(row).replace("'", "").replace("|", "'"))

        logger.info("Updated admin1 lookups")

        logger.info(f"Updating HDX resources")

        adm1_file = join(temp_folder, "polbnda_adm1_simplified.geojson")  # save file to disk
        adm1_json.to_file(adm1_file, driver="GeoJSON")

        centroid_lyr = GeoDataFrame(adm1_json.representative_point())  # convert to centroid
        centroid_lyr.rename(columns={0: "geometry"}, inplace=True)
        centroid_lyr[req_fields] = adm1_json[req_fields]
        centroid_lyr.set_geometry("geometry", inplace=True)
        centroid_file = join(temp_folder, "centroid_adm1.geojson")
        centroid_lyr.crs = None
        centroid_lyr.to_file(centroid_file, driver="GeoJSON")

        dataset = Dataset.read_from_hdx(configuration["boundaries"]["dataset"])  # update in HDX
        resource_a = find_resource(configuration["boundaries"]["dataset"], "geojson", kw="polbnda_adm1")[0]
        resource_a.set_file_to_upload(adm1_file)
        resource_c = find_resource(configuration["boundaries"]["dataset"], "geojson", kw="centroid_adm1")[0]
        resource_c.set_file_to_upload(centroid_file)

        try:
            dataset.update_in_hdx(
                remove_additional_resources=False,
                hxl_update=False,
                updated_by_script="HDX Scraper: Data Explorer inputs",
                ignore_fields=["num_of_rows"],
            )
        except HDXError:
            logger.error("Could not update boundary resource")

    # create regional bb geojson
    logger.info("Updating regional bbox jsons")
    for visualization in visualizations:

        regional_file = join("../saved_outputs", f"ocha-regions-bbox-{visualization}.geojson")
        with open(regional_file, "r") as f_open:
            regional_lyr = load(f_open)
        adm0_region = adm0_json[adm0_json["ISO_3"].isin(configuration["adm0"][visualization])]
        adm0_region["region"] = ""
        adm0_region["HRPs"] = ""
        adm0_region.loc[adm0_region["ISO_3"].isin(configuration["adm1"][visualization]), "HRPs"] = "HRPs"
        regional_info = configuration.get("regional")
        read_hdx_metadata(regional_info)
        _, iterator = read_tabular(downloader, regional_info)
        for row in iterator:
            adm0_region.loc[
                adm0_region["ISO_3"] == row[regional_info["iso3"]], "region"
            ] = row[regional_info["region"]]
        adm0_dissolve = adm0_region.dissolve(by="region")
        adm0_dissolve_HRPs = adm0_region[adm0_region["HRPs"] == "HRPs"].dissolve(by="HRPs")
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
        adm0_region = adm0_dissolve.to_json(show_bbox=True, drop_id=True)
        adm0_region = loads(adm0_region)
        regional_lyr["bbox"] = adm0_region["bbox"]
        for i in reversed(range(len(regional_lyr["features"]))):
            region = regional_lyr["features"][i]["properties"][
                "tbl_regcov_2020_ocha_Field3"
            ]
            k = [
                j
                for j in range(len(adm0_region["features"]))
                if adm0_region["features"][j]["properties"]["tbl_regcov_2020_ocha_Field3"]
                == region
            ]
            if len(k) != 1:
                logger.error(f"Cannot find region {region} in new boundaries")
                regional_lyr["features"].remove(regional_lyr["features"][i])
                continue
            regional_lyr["features"][i]["geometry"]["coordinates"] = adm0_region["features"][
                k[0]
            ]["geometry"]["coordinates"]
            regional_lyr["features"][i] = {
                "type": "Feature",
                "properties": regional_lyr["features"][i]["properties"],
                "bbox": adm0_region["features"][k[0]]["bbox"],
                "geometry": regional_lyr["features"][i]["geometry"],
            }
        replace_json(regional_lyr, regional_file)
    logger.info("Updated regional bbox jsons")

    # update boundaries in MapBox
    #logger.info("Updating MapBox boundaries")
    #for visualization in visualizations:
    #    update_mapbox(visualization, mapbox_auth, configuration)

    return countries
