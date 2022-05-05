import logging
import re
from os.path import join
from unicodedata import normalize
import topojson as tp
from geojson import loads
from geopandas import GeoDataFrame, read_file
from shapely.geometry import box

from hdx.scraper.utilities.readers import read_hdx_metadata, read_tabular
from hdx.location.country import Country
from scrapers.utilities.helper_functions import (
    download_unzip_read_data,
    find_resource,
    replace_json,
    drop_fields,
)
from scrapers.utilities.mapbox_functions import *

logger = logging.getLogger()


def update_boundaries(
        configuration,
        downloader,
        mapbox_auth,
        temp_folder,
        adm1_json,
        visualizations=None,
        countries=None,
):
    exceptions = configuration["boundaries"].get("dataset_exceptions")
    if not exceptions:
        exceptions = {}

    resource_exceptions = configuration["boundaries"].get("resource_exceptions")
    if not resource_exceptions:
        resource_exceptions = {}

    if not visualizations:
        visualizations = [key for key in configuration["adm0"]]

    if not countries:
        countries = []
    if len(countries) == 1 and countries[0].lower() == "all":
        countries = set()
        for visualization in visualizations:
            for iso in configuration["adm1"][visualization]:
                countries.add(iso)
        countries = list(countries)
        countries.sort()

    # download all mapbox datasets that are needed
    adm0_json = download_from_mapbox(configuration["mapbox"]["global"]["adm0-polbnda"], mapbox_auth)
    if isinstance(adm0_json, type(None)):
        return None
    adm0_json = GeoDataFrame.from_features(adm0_json["features"])

    adm0_l_json = download_from_mapbox(configuration["mapbox"]["global"]["adm0-polbndl"], mapbox_auth)
    if isinstance(adm0_l_json, type(None)):
        return None
    adm0_l_json = GeoDataFrame.from_features(adm0_l_json["features"])

    adm0_c_json = download_from_mapbox(configuration["mapbox"]["global"]["adm0-centroid"], mapbox_auth)
    if isinstance(adm0_c_json, type(None)):
        return None
    adm0_c_json = GeoDataFrame.from_features(adm0_c_json["features"])

    water_json = download_from_mapbox(configuration["mapbox"]["global"]["lakes"], mapbox_auth)
    if isinstance(water_json, type(None)):
        return None
    water_json = GeoDataFrame.from_features(water_json["features"])

    req_fields = ["alpha_3", "ADM0_REF", "ADM0_PCODE", "ADM1_REF", "ADM1_PCODE"]
    for iso in countries:
        if iso == "UKR":
            logger.info("Not processing UKR for now")
            continue

        logger.info(f"Processing admin1 boundaries for {iso}")

        country_adm0 = adm0_json.loc[(adm0_json["ISO_3"] == iso) | (adm0_json["Color_Code"] == iso)]
        country_adm0 = country_adm0.overlay(water_json, how="difference")
        country_adm0 = country_adm0.dissolve()
        country_adm0 = drop_fields(country_adm0, ["ISO_3"])
        country_adm0["ISO_3"] = iso
        if not country_adm0.crs:
            country_adm0 = country_adm0.set_crs(crs="EPSG:4326")

        dataset_name = exceptions.get(iso)
        resource_name = resource_exceptions.get(iso)
        if not resource_name:
            resource_name = "adm"
        boundary_resource = None
        if dataset_name:
            boundary_resource = find_resource(dataset_name, "SHP", kw=resource_name)
        if not boundary_resource:
            boundary_resource = find_resource(f"cod-em-{iso.lower()}", "SHP", kw=resource_name)
        if not boundary_resource:
            boundary_resource = find_resource(f"cod-ab-{iso.lower()}", "SHP", kw=resource_name)
        if not boundary_resource:
            logger.error(f"Could not find boundary dataset for {iso}")
            continue

        if len(boundary_resource) > 1:
            name_match = [
                bool(re.match(".*adm(in)?(\s)?(0)?1.*", r["name"], re.IGNORECASE))
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

        boundary_shp = download_unzip_read_data(boundary_resource[0], "shp", unzip=True)
        if not boundary_shp:
            continue

        if len(boundary_shp) > 1:
            name_match = [
                bool(re.match(".*admbnda.*adm(in)?(0)?1.*", b, re.IGNORECASE)) for b in boundary_shp
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

    centroid_lyr = GeoDataFrame(adm1_json.representative_point())  # convert to centroid
    centroid_lyr.rename(columns={0: "geometry"}, inplace=True)
    centroid_lyr[req_fields] = adm1_json[req_fields]
    centroid_lyr = centroid_lyr.set_geometry("geometry")

    adm1_line = GeoDataFrame(adm1_json.boundary)
    adm1_line = adm1_line.set_geometry(0)
    adm1_line["alpha_3"] = adm1_json["alpha_3"]
    adm1_line = adm1_line.overlay(adm0_l_json, how="difference")

    if len(countries) > 0:
        logger.info(f"Updating Mapbox datasets")
        adm1_json_to_upload = loads(adm1_json)
        adm1_line_to_upload = loads(adm1_line)
        adm1_centroid_to_upload = loads(centroid_lyr)
        replace_mapbox_dataset(configuration["mapbox"]["global"]["adm1-polbnda"], mapbox_auth, adm1_json_to_upload)
        replace_mapbox_dataset(configuration["mapbox"]["global"]["adm1-polbndl"], mapbox_auth, adm1_line_to_upload)
        replace_mapbox_dataset(configuration["mapbox"]["global"]["adm1-centroid"], mapbox_auth, adm1_centroid_to_upload)

    logger.info("Updating MapBox tilesets")
    for visualization in visualizations:
        replace_mapbox_tileset(
            configuration["mapbox"][visualization]["adm1-polbnda"],
            mapbox_auth,
            f"{visualization}-adm1-polbnda",
            json_to_upload=adm1_json[adm1_json["alpha_3"].isin(configuration["adm1"][visualization])],
            temp_folder=temp_folder,
        )
        replace_mapbox_tileset(
            configuration["mapbox"][visualization]["adm1-polbndl"],
            mapbox_auth,
            f"{visualization}-adm1-polbndl",
            json_to_upload=adm1_line[adm1_line["alpha_3"].isin(configuration["adm1"][visualization])],
            temp_folder=temp_folder,
        )
        replace_mapbox_tileset(
            configuration["mapbox"][visualization]["adm1-centroid"],
            mapbox_auth,
            f"{visualization}-adm1-centroid",
            json_to_upload=centroid_lyr[centroid_lyr["alpha_3"].isin(configuration["adm1"][visualization])],
            temp_folder=temp_folder,
        )
        to_upload = adm0_json.copy(deep=True)
        to_upload = to_upload[(to_upload["ISO_3"].isin(configuration["adm0"][visualization])) |
                              (to_upload["Color_Code"].isin(configuration["adm0"][visualization]))]
        to_upload.loc[to_upload["ISO_3"] == "XXX", "ISO_3"] = to_upload.loc[to_upload["ISO_3"] == "XXX", "Color_Code"]
        replace_mapbox_tileset(
            configuration["mapbox"][visualization]["adm0-polbnda"],
            mapbox_auth,
            f"{visualization}-adm0-polbnda",
            json_to_upload=to_upload,
            temp_folder=temp_folder,
        )
        replace_mapbox_tileset(
            configuration["mapbox"][visualization]["adm0-polbndl"],
            mapbox_auth,
            f"{visualization}-adm0-polbndl",
            json_to_upload=adm0_l_json[(adm0_l_json["BDY_CNT01"].isin(configuration["adm0"][visualization])) |
                                       (adm0_l_json["BDY_CNT02"].isin(configuration["adm0"][visualization]))],
            temp_folder=temp_folder,
        )
        replace_mapbox_tileset(
            configuration["mapbox"][visualization]["adm0-centroid"],
            mapbox_auth,
            f"{visualization}-adm0-centroid",
            json_to_upload=adm0_c_json["ISO_3"].isin(configuration["adm0"][visualization]),
            temp_folder=temp_folder,
        )

    for visualization in visualizations:  # update admin1 lookups
        logger.info(f"Updating admin1 lookups for {visualization}")
        attributes = list()
        for index, row in adm1_json.iterrows():
            if row["alpha_3"] in configuration["adm1"][visualization]:
                new_name = row["ADM1_REF"].replace("-", " ")
                new_name = normalize("NFKD", new_name).encode("ascii", "ignore").decode("ascii")
                attributes.append(
                    {
                        "country": row["ADM0_REF"],
                        "iso3": row["alpha_3"],
                        "pcode": row["ADM1_PCODE"],
                        "name": new_name,
                    }
                )
        attributes = sorted(attributes, key=lambda i: (i["country"], i["name"]))

        with open(join("saved_outputs", f"adm1-attributes-{visualization}.txt"), "w") as f:
            for row in attributes:
                if "," in row["name"]:
                    row["name"] = '"' + row["name"] + '"'
                if "'" in row["name"]:
                    row["name"] = row["name"].replace("'", "")
                f.write("- %s\n" % str(row).replace("'", "").replace("|", "'"))

    logger.info("Updated admin1 lookups")
    
    # create regional bb geojson
    logger.info("Updating regional bbox jsons")
    for visualization in visualizations:
        regional_file = join("saved_outputs", f"ocha-regions-bbox-{visualization}.geojson")

        adm0_region = adm0_json.copy(deep=True)
        adm0_region = adm0_region[adm0_region["ISO_3"].isin(configuration["adm0"][visualization])]
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
        adm0_region["name"] = "ocha regions - bbox"
        adm0_region["crs"] = {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}}
        replace_json(adm0_region, regional_file)

    logger.info("Updated regional bbox jsons")

    return countries
