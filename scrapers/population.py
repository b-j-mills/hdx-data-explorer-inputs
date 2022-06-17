import logging
import re
from os.path import join
from rasterstats import zonal_stats
from slugify import slugify

from hdx.location.country import Country
from hdx.data.hdxobject import HDXError
from scrapers.utilities.hdx_functions import download_unzip_read_data, find_resource, update_csv_resource

logger = logging.getLogger()


def update_population(
    configuration, downloader, adm1_countries, adm1_json, temp_folder, countries=None
):

    if not countries:
        countries = adm1_countries

    if len(countries) == 1 and countries[0].lower() == "all":
        countries = adm1_countries

    exceptions = configuration["population"].get("dataset_exceptions")
    if not exceptions:
        exceptions = {}

    resource_exceptions = configuration["population"].get("resource_exceptions")
    if not resource_exceptions:
        resource_exceptions = {}

    adm1_json["Population"] = None
    adm1_json.drop(adm1_json.index[~adm1_json["alpha_3"].isin(countries)], inplace=True)

    for iso in countries:
        logger.info(f"Processing population for {iso}")

        dataset_name = exceptions.get(iso)
        resource_name = resource_exceptions.get(iso)
        if not resource_name:
            resource_name = "adm(in)?1"
        if not dataset_name:
            dataset_name = f"cod-ps-{iso.lower()}"

        pop_resource = find_resource(dataset_name, "csv", kw=resource_name)
        if not pop_resource:
            dataset_name = f"worldpop-population-counts-for-{slugify(Country.get_country_name_from_iso3(iso))}"
            pop_resource = find_resource(dataset_name, "geotiff", kw="(?<!\d)\d{4}_constrained")
            if not pop_resource:
                logger.warning(f"Could not find any population data for {iso}")
                continue

            pop_raster = download_unzip_read_data(pop_resource[0], file_type="tif")
            if not pop_raster:
                continue

            pop_stats = zonal_stats(
                vectors=adm1_json.loc[(adm1_json["alpha_3"] == iso)],
                raster=pop_raster[0],
                stats="sum",
                geojson_out=True,
            )
            for row in pop_stats:
                pcode = row["properties"]["ADM1_PCODE"]
                pop = row["properties"]["sum"]
                if pop:
                    pop = int(round(pop, 0))
                    adm1_json.loc[adm1_json["ADM1_PCODE"] == pcode, "Population"] = pop

        if len(pop_resource) > 1:
            yearmatches = [re.findall("(?<!\d)\d{4}(?!\d)", r["name"], re.IGNORECASE) for r in pop_resource]
            yearmatches = sum(yearmatches, [])
            if len(yearmatches) > 0:
                yearmatches = [int(y) for y in yearmatches]
            maxyear = [r for r in pop_resource if str(max(yearmatches)) in r["name"]]
            if len(maxyear) == 1:
                pop_resource = maxyear

        if len(pop_resource) > 1:
            logger.warning(f"Found multiple resources for {iso}, using first in list")

        headers, iterator = downloader.get_tabular_rows(
            pop_resource[0]["url"], dict_form=True
        )

        pcode_header = None
        pop_header = []
        for header in headers:
            if not pcode_header:
                if (
                    header.upper()
                    in configuration["population_attribute_mappings"]["pcode"]
                ):
                    pcode_header = header
            if header.upper() in configuration["population_attribute_mappings"]["pop"]:
                pop_header.append(header)
            else:
                yearmatch = re.findall("(?<!\d)\d{4}(?!\d)", header, re.IGNORECASE)
                if len(yearmatch) > 0:
                    check_header = re.sub("(?<!\d)\d{4}(?!\d)", "Y", header, re.IGNORECASE)
                    if check_header.upper() in configuration["population_attribute_mappings"]["pop_with_years"]:
                        pop_header.append(header)

        if len(pop_header) > 1:
            yearmatches = [re.findall("(?<!\d)\d{4}(?!\d)", header, re.IGNORECASE) for header in pop_header]
            yearmatches = sum(yearmatches, [])
            if len(yearmatches) == 0:
                logger.info(f"Not sure which header to pick: {pop_header}")
                continue
            yearmatches = [int(y) for y in yearmatches]
            maxyear = [h for h in pop_header if str(max(yearmatches)) in h]
            if len(maxyear) != 1:
                logger.info(f"Not sure which header to pick: {pop_header}")
                continue
            pop_header = maxyear

        if not pcode_header:
            logger.error(f"Could not find pcode header for {iso}")
            continue
        if len(pop_header) != 1:
            logger.error(f"Could not find pop header for {iso}")
            continue
        pop_header = pop_header[0]

        for row in iterator:
            pcode = row[pcode_header]
            pop = row[pop_header]
            if pcode not in list(adm1_json["ADM1_PCODE"]):
                logger.info(f"Could not find unit {pcode} in boundaries for {iso}")
            else:
                adm1_json.loc[adm1_json["ADM1_PCODE"] == pcode, "Population"] = pop

    for index, row in adm1_json.iterrows():
        if not row["Population"]:
            logger.info(
                f"Could not find unit {row['ADM1_PCODE']} in statistics for {row['alpha_3']}"
            )

    adm1_json.drop(columns="geometry", inplace=True)
    adm1_json.sort_values(by=["ADM1_PCODE"], inplace=True)
    updated_countries = list(set(adm1_json["alpha_3"][~adm1_json["Population"].isna()]))
    resource = find_resource(configuration["population"]["dataset"], "csv")
    try:
        resource = resource[0]
    except IndexError:
        logger.error(f"Could not find population resource")
        return
    updated_resource = update_csv_resource(
        resource,
        downloader,
        adm1_json,
        updated_countries,
    )
    updated_resource.to_csv(join(temp_folder, "population_by_adm1.csv"), index=False)

    resource.set_file_to_upload(join(temp_folder, "population_by_adm1.csv"))
    try:
        resource.update_in_hdx()
    except HDXError:
        logger.exception("Could not update population resource")

    return
