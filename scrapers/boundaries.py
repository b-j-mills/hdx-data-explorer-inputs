import logging
import re
from os.path import join
from unicodedata import normalize
from geopandas import GeoDataFrame, read_file
from pandas import merge
from pandas.api.types import is_numeric_dtype
from shapely.geometry import box
from topojson import Topology

from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from scrapers.utilities.hdx_functions import download_unzip_read_data, find_resource
from scrapers.utilities.json_functions import *
from scrapers.utilities.mapbox_functions import *

logger = logging.getLogger()


def update_boundaries(
    configuration,
    downloader,
    mapbox_auth,
    temp_folder,
    adm1_json,
    data_source,
    update_tilesets,
    visualizations,
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
    if visualizations[0].lower() == "all":
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

    # download all datasets that are needed
    if data_source == "hdx":
        resource = find_resource(
            configuration["boundaries"]["dataset"], "geojson", kw="polbnda_int_1m"
        )
        if not resource:
            return
        adm0_json = download_unzip_read_data(
            resource[0], file_type="geojson", unzip=False, read=True
        )

        resource = find_resource(
            configuration["boundaries"]["dataset"], "geojson", kw="polbnda_int_15m"
        )
        if not resource:
            return
        adm0_json_lr = download_unzip_read_data(
            resource[0], file_type="geojson", unzip=False, read=True
        )

        resource = find_resource(
            configuration["boundaries"]["dataset"], "geojson", kw="polbndl_int"
        )
        if not resource:
            return
        adm0_l_json = download_unzip_read_data(
            resource[0], file_type="geojson", unzip=False, read=True
        )

        resource = find_resource(
            configuration["boundaries"]["dataset"], "geojson", kw="polbndp_int"
        )
        if not resource:
            return
        adm0_c_json = download_unzip_read_data(
            resource[0], file_type="geojson", unzip=False, read=True
        )

        resource = find_resource(
            configuration["boundaries"]["dataset"], "geojson", kw="lake"
        )
        if not resource:
            return
        water_json = download_unzip_read_data(
            resource[0], file_type="geojson", unzip=False, read=True
        )

    if data_source == "mapbox":
        adm0_json = download_from_mapbox(
            configuration["mapbox"]["global"]["polbnda_int_1m"], mapbox_auth
        )
        if isinstance(adm0_json, type(None)):
            return
        adm0_json = GeoDataFrame.from_features(adm0_json["features"])

        adm0_json_lr = download_from_mapbox(
            configuration["mapbox"]["global"]["polbnda_int_15m"], mapbox_auth
        )
        if isinstance(adm0_json_lr, type(None)):
            return
        adm0_json_lr = GeoDataFrame.from_features(adm0_json_lr["features"])

        adm0_l_json = download_from_mapbox(
            configuration["mapbox"]["global"]["polbndl_int"], mapbox_auth
        )
        if isinstance(adm0_l_json, type(None)):
            return
        adm0_l_json = GeoDataFrame.from_features(adm0_l_json["features"])

        adm0_c_json = download_from_mapbox(
            configuration["mapbox"]["global"]["polbndp_int"], mapbox_auth
        )
        if isinstance(adm0_c_json, type(None)):
            return
        adm0_c_json = GeoDataFrame.from_features(adm0_c_json["features"])

        water_json = download_from_mapbox(
            configuration["mapbox"]["global"]["lake"], mapbox_auth
        )
        if isinstance(water_json, type(None)):
            return
        water_json = GeoDataFrame.from_features(water_json["features"])

    req_fields = ["alpha_3", "ADM0_REF", "ADM0_PCODE", "ADM1_REF", "ADM1_PCODE"]
    for iso in countries:
        if iso == "UKR":
            logger.info("Not processing UKR for now")
            continue

        logger.info(f"Processing admin1 boundaries for {iso}")

        # select single country boundary (including disputed areas), cut out water, and dissolve
        country_adm0 = adm0_json.copy(deep=True)
        country_adm0 = country_adm0.loc[
            (country_adm0["ISO_3"] == iso) | (country_adm0["Color_Code"] == iso)
        ]
        country_adm0 = country_adm0.overlay(water_json, how="difference")
        country_adm0 = country_adm0.dissolve()
        country_adm0 = drop_fields(country_adm0, ["ISO_3"])
        country_adm0["ISO_3"] = iso
        if not country_adm0.crs:
            country_adm0 = country_adm0.set_crs(crs="EPSG:4326")

        # find the correct admin boundary resource
        dataset_name = exceptions.get(iso)
        resource_name = resource_exceptions.get(iso)
        if not resource_name:
            resource_name = "adm"
        boundary_resource = None
        if dataset_name:
            boundary_resource = find_resource(dataset_name, "SHP", kw=resource_name)
        if not boundary_resource:
            boundary_resource = find_resource(
                f"cod-em-{iso.lower()}", "SHP", kw=resource_name
            )
        if not boundary_resource:
            boundary_resource = find_resource(
                f"cod-ab-{iso.lower()}", "SHP", kw=resource_name
            )
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

        # find the correct admin boundary shapefile in the downloaded zip
        boundary_shp = download_unzip_read_data(boundary_resource[0], "shp", unzip=True)
        if not boundary_shp:
            continue

        if len(boundary_shp) > 1:
            name_match = [
                bool(re.match(".*admbnda.*adm(in)?(0)?1.*", b, re.IGNORECASE))
                for b in boundary_shp
            ]
            if any(name_match):
                boundary_shp = [
                    boundary_shp[i] for i in range(len(boundary_shp)) if name_match[i]
                ]

        if len(boundary_shp) > 1:
            simp_match = [
                bool(re.match(".*simplified.*", b, re.IGNORECASE)) for b in boundary_shp
            ]
            if any(simp_match):
                boundary_shp = [
                    boundary_shp[i]
                    for i in range(len(boundary_shp))
                    if not simp_match[i]
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

        # calculate fields, finding admin1 name and pcode fields from config
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

        if not name_field:
            logger.error(f"Could not map name field for {iso}")
            continue

        if not pcode_field:
            boundary_lyr["ADM1_PCODE"] = ""

        # calculate text pcodes (if pcod is not in field name or there were no codes in the boundaries, create pcodes)
        if pcode_field:
            if is_numeric_dtype(boundary_lyr[pcode_field]):
                if "PCOD" not in pcode_field:
                    numrows = len(str(len(boundary_lyr.index)))
                    for i, _ in boundary_lyr.iterrows():
                        boundary_lyr.loc[i, "ADM1_PCODE"] = boundary_lyr.loc[
                            i, "ADM0_PCODE"
                        ] + str(int(boundary_lyr.loc[i, pcode_field])).zfill(numrows)
                else:
                    boundary_lyr["ADM1_PCODE"] = (
                        boundary_lyr[pcode_field].astype(int).astype(str)
                    )
            else:
                boundary_lyr["ADM1_PCODE"] = boundary_lyr[pcode_field]

        boundary_lyr["ADM1_REF"] = boundary_lyr[name_field]
        boundary_lyr = drop_fields(boundary_lyr, req_fields)
        boundary_lyr = boundary_lyr.dissolve(by=req_fields, as_index=False)

        if not pcode_field:
            logger.error(f"Could not map pcodes - assigning randomly!")
            numrows = len(str(len(boundary_lyr.index)))
            for i, _ in boundary_lyr.iterrows():
                boundary_lyr.loc[i, "ADM1_PCODE"] = boundary_lyr.loc[
                    i, "ADM0_PCODE"
                ] + str(i + 1).zfill(numrows)

        na_count = boundary_lyr["ADM1_REF"].isna().sum()
        if na_count > 0:
            logger.warning(f"Found {na_count} null values in {iso} boundary")

        # simplify geometry of boundaries
        # TODO: possibly identify small scale boundaries that don't need simplifying
        boundary_topo = Topology(boundary_lyr)
        boundary_topo = boundary_topo.toposimplify(
            epsilon=0.01,
            simplify_algorithm="dp",
            prevent_oversimplify=True,
        )
        boundary_lyr = boundary_topo.to_gdf(crs="EPSG:4326")

        # harmonize international boundary with UN admin0 country boundary
        boundary_union = boundary_lyr.overlay(country_adm0, how="union")
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

    adm1_json.sort_values(by=["ADM1_PCODE"], inplace=True)

    # convert polygon boundaries to point
    adm1_centroid = GeoDataFrame(adm1_json.representative_point())
    adm1_centroid.rename(columns={0: "geometry"}, inplace=True)
    adm1_centroid[req_fields] = adm1_json[req_fields]
    adm1_centroid = adm1_centroid.set_geometry("geometry")

    if len(countries) > 0:
        if data_source == "hdx":
            logger.info(f"Updating HDX datasets")
            adm1_file = join(temp_folder, "polbnda_adm1_1m_ocha.geojson")
            adm1_json.to_file(adm1_file, driver="GeoJSON")
            centroid_file = join(temp_folder, "polbndp_adm1_1m_ocha.geojson")
            adm1_centroid.to_file(centroid_file, driver="GeoJSON")

            resource_a = find_resource(
                configuration["boundaries"]["dataset"], "geojson", kw="polbnda_adm1"
            )[0]
            resource_a.set_file_to_upload(adm1_file)
            resource_c = find_resource(
                configuration["boundaries"]["dataset"], "geojson", kw="polbndp_adm1"
            )[0]
            resource_c.set_file_to_upload(centroid_file)

            try:
                resource_a.update_in_hdx()
            except HDXError:
                logger.exception("Could not update polygon resource")
            try:
                resource_c.update_in_hdx()
            except HDXError:
                logger.exception("Could not update point resource")

        if data_source == "mapbox":
            logger.info(f"Updating Mapbox datasets")
            replace_mapbox_dataset(
                configuration["mapbox"]["global"]["polbnda_adm1"],
                mapbox_auth,
                json_to_upload=adm1_json,
            )
            replace_mapbox_dataset(
                configuration["mapbox"]["global"]["polbndp_adm1"],
                mapbox_auth,
                json_to_upload=adm1_centroid,
            )

    if update_tilesets:
        logger.info("Updating MapBox tilesets")
        for visualization in visualizations:
            replace_mapbox_tileset(
                configuration["mapbox"][visualization]["polbnda_adm1"]["mapid"],
                mapbox_auth,
                configuration["mapbox"][visualization]["polbnda_adm1"]["name"],
                json_to_upload=adm1_json[
                    adm1_json["alpha_3"].isin(configuration["adm1"][visualization])
                ],
                temp_folder=temp_folder,
            )
            replace_mapbox_tileset(
                configuration["mapbox"][visualization]["polbndp_adm1"]["mapid"],
                mapbox_auth,
                configuration["mapbox"][visualization]["polbndp_adm1"]["name"],
                json_to_upload=adm1_centroid[
                    adm1_centroid["alpha_3"].isin(configuration["adm1"][visualization])
                ],
                temp_folder=temp_folder,
            )
            # admin0 boundaries should include disputed areas and be dissolved to single features
            # to_upload = adm0_json.copy(deep=True)
            to_upload = adm0_json_lr.copy(deep=True)
            to_upload = to_upload[
                (to_upload["ISO_3"].isin(configuration["adm0"][visualization]))
                | (to_upload["Color_Code"].isin(configuration["adm0"][visualization]))
            ]
            to_upload.loc[to_upload["ISO_3"] == "XXX", "ISO_3"] = to_upload.loc[
                to_upload["ISO_3"] == "XXX", "Color_Code"
            ]
            to_upload.loc[to_upload["ISO_3"].isna(), "ISO_3"] = to_upload.loc[
                to_upload["ISO_3"].isna(), "Color_Code"
            ]
            to_upload = to_upload.dissolve(by="ISO_3")
            to_upload["ISO_3"] = to_upload.index
            to_upload.reset_index(drop=True, inplace=True)
            to_upload = drop_fields(to_upload, ["ISO_3"])
            to_upload = merge(
                to_upload,
                adm0_json_lr[["ISO_3", "STATUS", "Color_Code", "Terr_ID", "Terr_Name"]],
                on="ISO_3",
            )
            replace_mapbox_tileset(
                configuration["mapbox"][visualization]["polbnda_int"]["mapid"],
                mapbox_auth,
                configuration["mapbox"][visualization]["polbnda_int"]["name"],
                json_to_upload=to_upload,
                temp_folder=temp_folder,
            )
            replace_mapbox_tileset(
                configuration["mapbox"][visualization]["polbndl_int"]["mapid"],
                mapbox_auth,
                configuration["mapbox"][visualization]["polbndl_int"]["name"],
                json_to_upload=adm0_l_json[
                    (adm0_l_json["BDY_CNT01"].isin(configuration["adm0"][visualization]))
                    | (adm0_l_json["BDY_CNT02"].isin(configuration["adm0"][visualization]))
                ],
                temp_folder=temp_folder,
            )
            replace_mapbox_tileset(
                configuration["mapbox"][visualization]["polbndp_int"]["mapid"],
                mapbox_auth,
                configuration["mapbox"][visualization]["polbndp_int"]["name"],
                json_to_upload=adm0_c_json[
                    adm0_c_json["ISO_3"].isin(configuration["adm0"][visualization])
                ],
                temp_folder=temp_folder,
            )

    for visualization in visualizations:  # update admin1 lookups
        logger.info(f"Updating admin1 lookups for {visualization}")
        attributes = list()
        for _, row in adm1_json.iterrows():
            if row["alpha_3"] in configuration["adm1"][visualization]:
                new_name = row["ADM1_REF"].replace("-", " ").replace("`", "").replace("'", "")
                new_name = (
                    normalize("NFKD", new_name)
                    .encode("ascii", "ignore")
                    .decode("ascii")
                )
                attributes.append(
                    {
                        "country": row["ADM0_REF"],
                        "iso3": row["alpha_3"],
                        "pcode": row["ADM1_PCODE"],
                        "name": new_name,
                    }
                )
        attributes = sorted(attributes, key=lambda i: (i["country"], i["name"]))

        with open(
            join("saved_outputs", f"adm1-attributes-{visualization}.txt"), "w"
        ) as f:
            for row in attributes:
                if "," in row["name"]:
                    row["name"] = '"' + row["name"] + '"'
                f.write("- %s\n" % str(row).replace("'", "").replace("|", "'"))

    logger.info("Updated admin1 lookups")

    logger.info("Updating regional bbox jsons")
    for visualization in visualizations:
        regional_file = join(
            "saved_outputs", f"ocha-regions-bbox-{visualization}.geojson"
        )

        # select only territories or disputed areas in visualization
        adm0_region = adm0_json.copy(deep=True)
        adm0_region = adm0_region[
            (adm0_region["ISO_3"].isin(configuration["adm0"][visualization]))
            | (adm0_region["Color_Code"].isin(configuration["adm0"][visualization]))
        ]
        adm0_region.loc[adm0_region["ISO_3"] == "XXX", "ISO_3"] = adm0_region.loc[
            adm0_region["ISO_3"] == "XXX", "Color_Code"
        ]
        adm0_region.loc[adm0_region["ISO_3"].isna(), "ISO_3"] = adm0_region.loc[
            adm0_region["ISO_3"].isna(), "Color_Code"
        ]
        # assign region and HRP status
        adm0_region["region"] = ""
        adm0_region["HRPs"] = ""
        adm0_region.loc[
            adm0_region["ISO_3"].isin(configuration["HRPs"]), "HRPs"
        ] = "HRPs"
        regional_info = configuration["regional"]
        resource = find_resource(regional_info["dataset"], regional_info["format"])
        _, iterator = downloader.get_tabular_rows(resource[0]["url"], dict_form=True)
        for row in iterator:
            adm0_region.loc[
                adm0_region["ISO_3"] == row[regional_info["iso3"]], "region"
            ] = row[regional_info["region"]]
        # dissolve boundaries by region and HRP
        adm0_dissolve = adm0_region.dissolve(by="region")
        adm0_dissolve_HRPs = adm0_region[adm0_region["HRPs"] == "HRPs"].dissolve(
            by="HRPs"
        )
        adm0_dissolve = adm0_dissolve.append(adm0_dissolve_HRPs)
        # convert to bounding box
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
        # remove any "NO_COVERAGE" regions
        for i in reversed(range(len(adm0_region["features"]))):
            if (
                adm0_region["features"][i]["properties"]["tbl_regcov_2020_ocha_Field3"] == "NO COVERAGE"
            ):
                adm0_region["features"].remove(adm0_region["features"][i])
        adm0_region["name"] = "ocha regions - bbox"
        adm0_region["crs"] = {
            "type": "name",
            "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"},
        }
        replace_json(adm0_region, regional_file)

    logger.info("Updated regional bbox jsons")

    return
