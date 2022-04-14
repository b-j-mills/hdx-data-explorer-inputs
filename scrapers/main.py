import logging
from geopandas import read_file

#from scrapers.boundaries import update_boundaries
from scrapers.health_facilities import update_health_facilities
from scrapers.population import update_population
from scrapers.utilities.helper_functions import find_resource

logger = logging.getLogger(__name__)


def get_indicators(
    configuration,
    retriever,
    mapbox_auth,
    scrapers_to_run=None,
    countries=None,
    visualizations=None,
    errors_on_exit=None,
):
    downloader = retriever.downloader
    temp_folder = retriever.temp_dir

    if not scrapers_to_run:
        scrapers_to_run = ["boundaries", "health_facilities", "population"]

    adm1_countries = set()
    for viz in configuration["adm1"]:
        for country in configuration["adm1"][viz]:
            adm1_countries.add(country)
    adm1_countries = list(adm1_countries)

    resource = find_resource(configuration["boundaries"]["dataset"], "geojson", kw="adm1")
    if not resource:
        logger.error(f"Could not find admin1 geojson!")
        return None
    url, path = resource[0].download()

    adm1_json = read_file(path)
    adm1_json.sort_values(by=["ADM1_PCODE"], inplace=True)

    if "boundaries" in scrapers_to_run:
        resource = find_resource(configuration["boundaries"]["dataset"], "geojson", kw="wrl_polbnda")
        if not resource:
            logger.error(f"Could not find admin1 geojson!")
            return None
        url, path = resource[0].download()
        adm0_json = read_file(path)

        boundaries = update_boundaries(
            configuration,
            downloader,
            mapbox_auth,
            temp_folder,
            adm0_json,
            adm1_json,
            visualizations,
            countries,
        )
    if "population" in scrapers_to_run:
        population = update_population(
            configuration,
            downloader,
            adm1_countries,
            adm1_json,
            temp_folder,
            countries,
        )
    if "health_facilities" in scrapers_to_run:
        health_facilities = update_health_facilities(
            configuration,
            downloader,
            adm1_countries,
            adm1_json,
            temp_folder,
            countries,
        )

    return adm1_countries
