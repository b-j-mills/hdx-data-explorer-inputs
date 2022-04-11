import logging
from os.path import join
from geopandas import read_file
from pandas import DataFrame

from hdx.data.dataset import Dataset
from scrapers.utilities.helper_functions import find_resource, download_unzip_data, update_csv_resource

logger = logging.getLogger()


def update_health_facilities(
    configuration, downloader, adm1_countries, adm1_json, temp_folder, countries=None
):
    configuration = configuration["health_facilities"]

    if not countries:
        countries = adm1_countries

    if len(countries) == 1 and countries[0].lower() == "all":
        countries = adm1_countries

    exceptions = configuration.get("exceptions")
    if not exceptions:
        exceptions = {}

    adm1_json["Health_Facilities"] = None
    adm1_json.drop(adm1_json.index[~adm1_json["alpha_3"].isin(countries)], inplace=True)

    for iso in countries:
        logger.info(f"Processing health facilities for {iso}")

        dataset_name = exceptions.get(iso)
        if not dataset_name:
            dataset_name = f"hotosm_{iso.lower()}_health_facilities"

        health_resource = find_resource(dataset_name, "shp", kw="point")
        if not health_resource:
            continue

        health_shp = download_unzip_data(downloader, health_resource[0], "shp")
        if not health_shp:
            continue

        health_shp = health_shp[0]

        health_shp_lyr = read_file(health_shp)
        join_lyr = health_shp_lyr.sjoin(adm1_json)
        join_lyr = DataFrame(join_lyr)
        join_lyr = join_lyr.groupby("ADM1_PCODE").size()
        for pcode in join_lyr.index:
            hfs = join_lyr[pcode]
            adm1_json.loc[
                adm1_json["ADM1_PCODE"] == pcode, "health_facility_count"
            ] += hfs

    adm1_json = adm1_json.drop(columns="geometry")

    dataset = Dataset.read_from_hdx(configuration["dataset"])
    resource = dataset.get_resource(0)
    updated_resource = update_csv_resource(resource, downloader, adm1_json, countries)

    updated_resource.to_csv(join(temp_folder, "health_facilities_by_adm1.csv"), index=False)

    resource.set_file_to_upload(join(temp_folder, "health_facilities_by_adm1.csv"))

    dataset.update_in_hdx(
        remove_additional_resources=True,
        hxl_update=False,
        updated_by_script="HDX Scraper: Data Explorer inputs",
        ignore_fields=["num_of_rows"],
    )
    return countries
