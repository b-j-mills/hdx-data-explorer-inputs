import logging
import re
from os.path import join

from hdx.data.hdxobject import HDXError
from scrapers.utilities.hdx_functions import find_resource, update_csv_resource

logger = logging.getLogger()


def update_population(
    configuration, downloader, adm1_countries, adm1_json, temp_folder, countries=None
):

    configuration = configuration["population"]

    if not countries:
        countries = adm1_countries

    if len(countries) == 1 and countries[0].lower() == "all":
        countries = adm1_countries

    exceptions = configuration.get("dataset_exceptions")
    if not exceptions:
        exceptions = {}

    resource_exceptions = configuration.get("resource_exceptions")
    if not resource_exceptions:
        resource_exceptions = {}

    adm1_json["Population"] = None
    adm1_json.drop(adm1_json.index[~adm1_json["alpha_3"].isin(countries)], inplace=True)

    for iso in countries:
        logger.info(f"Processing population for {iso}")

        dataset_name = exceptions.get(iso)
        resource_name = resource_exceptions.get(iso)
        if not resource_name:
            resource_name = "adm1"
        if not dataset_name:
            dataset_name = f"cod-ps-{iso.lower()}"

        pop_resource = find_resource(dataset_name, "csv", kw=resource_name)
        if not pop_resource:
            continue

        if len(pop_resource) > 1:
            logger.info(f"Found multiple resources for {iso}")

        headers, iterator = downloader.get_tabular_rows(pop_resource[0]["url"], dict_form=True)

        pcode_header = None
        pop_header = None
        for header in headers:
            if not pcode_header:
                codematch = bool(re.match(".*p?code.*", header, re.IGNORECASE))
                levelmatch = bool(re.match(".*1.*", header, re.IGNORECASE))
                if codematch and levelmatch:
                    pcode_header = header
            popmatch = bool(
                re.search(
                    "(population|both|total|proj|pop|ensemble)",
                    header,
                    re.IGNORECASE,
                )
            )
            tmatch = bool(re.search("t", header, re.IGNORECASE))
            sexyearmatch = bool(
                re.search("_f|_m|m_|f_|year|female|male", header, re.IGNORECASE)
            )
            agematch = bool(
                re.search("^\d{1,2}\D|(\D\d{1,2}\D)|(\D\d$)", header, re.IGNORECASE)
            )
            agewordmatch = bool(re.search("(age|adult)", header, re.IGNORECASE))
            yearmatch = len(re.findall("\d{4}", header, re.IGNORECASE))
            if (
                (popmatch or tmatch)
                and not sexyearmatch
                and not agematch
                and not agewordmatch
                and yearmatch < 2
            ):
                if not pop_header:
                    pop_header = header
                elif popmatch:
                    pop_header = header
                if pop_header and yearmatch > 0:
                    try:
                        h1year = re.search("20\d{2}", pop_header).group(0)
                    except AttributeError:
                        logger.info(
                            f"Not sure which header to pick: {pop_header}, {header}"
                        )
                        continue
                    h2year = re.search("20\d{2}", header).group(0)
                    if int(h2year) > int(h1year):
                        pop_header = header

        if not pcode_header:
            logger.error(f"Could not find pcode header for {iso}")
            continue
        if not pop_header:
            logger.error(f"Could not find pop header for {iso}")
            continue

        for row in iterator:
            pcode = row[pcode_header]
            pop = row[pop_header]
            if pcode not in list(adm1_json["ADM1_PCODE"]):
                logger.info(f"Could not find unit {pcode} in boundaries for {iso}")
            else:
                adm1_json.loc[adm1_json["ADM1_PCODE"] == pcode, "Population"] = pop

    for index, row in adm1_json.iterrows():
        if not row["Population"]:
            logger.info(f"Could not find unit {row['ADM1_PCODE']} in statistics for {row['alpha_3']}")

    adm1_json.drop(columns="geometry", inplace=True)
    adm1_json.sort_values(by=["ADM1_PCODE"], inplace=True)
    updated_countries = list(set(adm1_json["alpha_3"][~adm1_json["Population"].isna()]))
    resource = find_resource(configuration["dataset"], "csv")[0]
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

    return countries
