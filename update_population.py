import logging
import re
from os.path import join
from geopandas import read_file

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.facades.simple import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from helper_functions import copy_files_to_archive, find_resource

setup_logging()
logger = logging.getLogger()


def main():
    configuration = Configuration.read()

    HRPs = configuration["HRPs"]
    data_category = "population"
    copy_files_to_archive(data_category)

    admin1_json = read_file(
        join(
            "Geoprocessing",
            "latest",
            "adm1",
            "hrps_polbnda_adm1(ish)_simplified_ocha.geojson",
        )
    )
    admin1_json.drop(
        admin1_json.index[~admin1_json["alpha_3"].isin(HRPs)], inplace=True
    )
    admin1_json.drop(columns="geometry", inplace=True)
    admin1_json = admin1_json.sort_values(by=["ADM1_PCODE"])
    admin1_json["population"] = None

    for iso in HRPs:
        logger.info(f"Processing {iso}")
        dataset_name = f"cod-ps-{iso.lower()}"

        pop_resource = find_resource(iso, dataset_name, "csv", kw="adm1")
        if not pop_resource:
            continue

        with Download() as downloader:
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
                admin1_json.loc[admin1_json["ADM1_PCODE"] == pcode, "population"] = pop

    admin1_json.to_csv(
        join("Geoprocessing", "latest", "population", "population_by_admin1.csv"),
        index=False,
    )

    dataset = Dataset.read_from_hdx("dataset_name")
    resource = dataset.get_resource(0)
    resource.set_file_to_upload(
        join("Geoprocessing", "latest", "population", "population_by_admin1.csv")
    )
    dataset.update_in_hdx(
        remove_additional_resources=True,
        hxl_update=False,
        updated_by_script="HDX Scraper: COVID viz inputs",
        ignore_fields=["num_of_rows"],
    )


if __name__ == "__main__":
    facade(
        main,
        hdx_read_only=True,
        hdx_site="prod",
        user_agent="hdx-covid-viz-data-inputs",
        preprefix="PREPREFIX",
        project_config_yaml=join("config", "project_configuration.yml"),
    )
