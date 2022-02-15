import logging
from os.path import join
from geopandas import read_file, sjoin
from pandas import DataFrame
from helper_functions import copy_files_to_archive, retrieve_data, unzip_data

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.facades.simple import facade
from hdx.utilities.easy_logging import setup_logging

setup_logging()
logger = logging.getLogger()


def main():
    configuration = Configuration.read()

    HRPs = configuration["HRPs"]
    health_facilities = configuration["health_facilities"]
    data_category = "health_facilities"
    copy_files_to_archive(data_category)
    admin1_json = read_file(
        join("Geoprocessing", "latest", "adm1", "hrps_polbnda_adm1(ish)_simplified_ocha.geojson")
    )

    admin1_json.drop(admin1_json.index[~admin1_json["alpha_3"].isin(HRPs)], inplace=True)
    admin1_json = admin1_json.sort_values(by=["ADM1_PCODE"])
    admin1_json["health_facility_count"] = 0

    for iso in HRPs:
        logger.info(f"Processing {iso}")
        dataset_name = health_facilities.get(iso.upper())
        if not dataset_name:
            logger.error(f"No health facilities for {iso}")
            continue

        health_zip = retrieve_data(iso, dataset_name, data_category, "shp")
        if not health_zip:
            continue

        health_shp = unzip_data(iso, health_zip, data_category, "shp")
        if not health_shp:
            continue

        health_shp_lyr = read_file(health_shp)
        join_lyr = sjoin(health_shp_lyr, admin1_json)
        join_lyr = DataFrame(join_lyr)
        join_lyr = join_lyr.groupby("ADM1_PCODE").size()
        for pcode in join_lyr.index:
            hfs = join_lyr[pcode]
            admin1_json.loc[admin1_json["ADM1_PCODE"] == pcode, "health_facility_count"] += hfs

    admin1_json = admin1_json.drop(columns="geometry")
    admin1_json.to_csv(
        join("Geoprocessing", "latest", "health_facilities", "health_facilities_by_admin1.csv"),
        index=False,
    )

    # dataset = Dataset.read_from_hdx("dataset_name")
    # resource = Dataset.get_resource(0)
    # resource.set_file_to_upload(join("Geoprocessing", "latest", "health_facilities", "health_facilities_by_admin1.csv"))
    # dataset.update_in_hdx(
    #     remove_additional_resources=True,
    #     hxl_update=False,
    #     updated_by_script="HDX Scraper: CODS",
    #     ignore_fields=["num_of_rows", "resource:description"],
    # )


if __name__ == "__main__":
    facade(
        main,
        hdx_read_only=True,
        hdx_site="prod",
        user_agent="hdx-covid-viz-data-inputs",
        preprefix="PREPREFIX",
        project_config_yaml=join("config", "project_configuration.yml"),
    )
