import logging
import re
from os.path import join

from hdx.data.dataset import Dataset
from scrapers.utilities.helper_functions import find_resource, update_csv_resource

logger = logging.getLogger()


def update_population(
    configuration, downloader, adm1_countries, adm1_json, temp_folder, countries=None
):

    configuration = configuration["population"]

    if not countries:
        countries = adm1_countries

    exceptions = configuration.get("exceptions")
    if not exceptions:
        exceptions = {}

    adm1_json["Population"] = None
    adm1_json.drop(adm1_json.index[~adm1_json["alpha_3"].isin(countries)], inplace=True)

    for iso in countries:
        logger.info(f"Processing population for {iso}")

        dataset_name = exceptions.get(iso)
        if not dataset_name:
            dataset_name = f"cod-ps-{iso.lower()}"

        pop_resource = find_resource(dataset_name, "csv", kw="adm1")
        if not pop_resource:
            logger.info(f"No CODs pop data for {iso}")
            continue

        headers, iterator = downloader.get_tabular_rows(
            pop_resource["url"], headers=1, dict_form=True, format="csv"
        )

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
                    "(population|both|total|proj|pop|t|ensemble)",
                    header,
                    re.IGNORECASE,
                )
            )
            sexyearmatch = bool(
                re.search("_f|_m|m_|f_|year|female|male", header, re.IGNORECASE)
            )
            agematch = bool(
                re.search("^\d{1,2}\D|(\D\d{1,2}\D)|(\D\d$)", header, re.IGNORECASE)
            )
            agewordmatch = bool(re.search("(age|adult)", header, re.IGNORECASE))
            yearmatch = len(re.findall("\d{4}", header, re.IGNORECASE))
            if (
                popmatch
                and not sexyearmatch
                and not agematch
                and not agewordmatch
                and yearmatch < 2
            ):
                if not pop_header:
                    pop_header = header
                elif yearmatch > 0:
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
            adm1_json.loc[adm1_json["ADM1_PCODE"] == pcode, "Population"] = pop

    adm1_json.drop(columns="geometry", inplace=True)

    dataset = Dataset.read_from_hdx(configuration["dataset"])
    resource = dataset.get_resource(0)
    updated_resource = update_csv_resource(resource, downloader, adm1_json, countries)

    updated_resource.to_csv(join(temp_folder, "population_by_adm1.csv"), index=False)

    resource.set_file_to_upload(join(temp_folder, "population_by_adm1.csv"))

    dataset.update_in_hdx(
        remove_additional_resources=True,
        hxl_update=False,
        updated_by_script="HDX Scraper: Data Explorer inputs",
        ignore_fields=["num_of_rows"],
    )
    return
