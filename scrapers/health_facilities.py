import logging
from os.path import join
from pandas import DataFrame

from hdx.data.hdxobject import HDXError
from scrapers.utilities.helper_functions import (
    find_resource,
    download_unzip_read_data,
    update_csv_resource,
)

logger = logging.getLogger()


def update_health_facilities(
    configuration, downloader, adm1_countries, adm1_json, temp_folder, countries=None
):
    configuration = configuration["health_facilities"]

    if not countries:
        countries = adm1_countries

    if len(countries) == 1 and countries[0].lower() == "all":
        countries = adm1_countries

    exceptions = configuration.get("dataset_exceptions")
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

        health_shp_lyr = download_unzip_read_data(health_resource[0], "shp", unzip=True, read=True)
        if isinstance(health_shp_lyr, type(None)):
            continue

        join_lyr = health_shp_lyr.sjoin(adm1_json)
        join_lyr = DataFrame(join_lyr)
        join_lyr = join_lyr.groupby("ADM1_PCODE").size()
        for pcode in join_lyr.index:
            hfs = join_lyr[pcode]
            adm1_json.loc[
                adm1_json["ADM1_PCODE"] == pcode, "Health_Facilities"
            ] += hfs

    adm1_json = adm1_json.drop(columns="geometry")
    resource = find_resource(configuration["dataset"], "csv")[0]
    updated_resource = update_csv_resource(
        resource,
        downloader,
        adm1_json,
        list(set(adm1_json["alpha_3"][~adm1_json["Health_Facilities"].isna()])),
    )
    updated_resource.to_csv(join(temp_folder, "health_facilities_by_adm1.csv"), index=False)

    resource.set_file_to_upload(join(temp_folder, "health_facilities_by_adm1.csv"))
    try:
        resource.update_in_hdx()
    except HDXError:
        logger.exception("Could not update health facilities resource")

    return countries
