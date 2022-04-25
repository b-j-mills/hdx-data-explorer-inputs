import argparse
import logging
from os import getenv
from os.path import join

from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from scrapers.main import get_indicators

setup_logging()
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ua", "--user_agent", default=None, help="user agent")
    parser.add_argument("-pp", "--preprefix", default=None, help="preprefix")
    parser.add_argument("-hs", "--hdx_site", default=None, help="HDX site to use")
    parser.add_argument("-sc", "--scrapers", default=None, help="Scrapers to run")
    parser.add_argument("-co", "--countries", default=None, help="Which countries to update")
    parser.add_argument("-vi", "--visualizations", default=None, help="Which visualizations to update")
    parser.add_argument("-ma", "--mapbox_auth", default=None, help="Credentials for accessing MapBox data")
    args = parser.parse_args()
    return args


def main(
        scrapers_to_run,
        countries,
        visualizations,
        mapbox_auth,
        **ignore,
):
    logger.info(f"##### hdx-viz-data-inputs ####")
    configuration = Configuration.read()
    with ErrorsOnExit() as errors_on_exit:
        with temp_dir() as temp_folder:
            with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
                if scrapers_to_run:
                    logger.info(f"Updating only scrapers: {scrapers_to_run}")
                countries_to_save = get_indicators(
                    configuration,
                    downloader,
                    temp_folder,
                    scrapers_to_run,
                    countries,
                    visualizations,
                    mapbox_auth,
                    errors_on_exit,
                )


if __name__ == "__main__":
    args = parse_args()
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv("USER_AGENT")
        if user_agent is None:
            user_agent = "hdx-scraper-data-explorers"
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv("PREPREFIX")
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv("HDX_SITE", "prod")
    if args.scrapers:
        scrapers_to_run = args.scrapers.split(",")
    else:
        scrapers_to_run = None
    if args.countries:
        countries = args.countries.split(",")
    else:
        countries = None
    if args.visualizations:
        visualizations = args.visualizations.split(",")
    else:
        visualizations = None
    mapbox_auth = args.mapbox_auth
    if mapbox_auth is None:
        mapbox_auth = getenv("MAPBOX_AUTH")
    facade(
        main,
        user_agent=user_agent,
        preprefix=preprefix,
        hdx_site=hdx_site,
        project_config_yaml=join("config", "project_configuration.yml"),
        scrapers_to_run=scrapers_to_run,
        countries=countries,
        visualizations=visualizations,
        mapbox_auth=mapbox_auth,
    )
