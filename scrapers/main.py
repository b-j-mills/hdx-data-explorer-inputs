import logging

from scrapers.un_boundaries import update_un_boundaries
from scrapers.boundaries import update_boundaries
from scrapers.health_facilities import update_health_facilities
from scrapers.population import update_population
from scrapers.utilities.helper_functions import find_resource, download_unzip_read_data

logger = logging.getLogger(__name__)


def get_indicators(
        configuration,
        downloader,
        temp_folder,
        scrapers_to_run=None,
        countries=None,
        visualizations=None,
        mapbox_auth=None,
):

    if not scrapers_to_run:
        scrapers_to_run = ["boundaries", "health_facilities", "population"]

    adm1_countries = set()
    for viz in configuration["adm1"]:
        for country in configuration["adm1"][viz]:
            adm1_countries.add(country)
    adm1_countries = list(adm1_countries)
    adm1_countries.sort()

    resource = find_resource(configuration["boundaries"]["dataset"], "geojson", kw="polbnda_adm1")
    if not resource:
        logger.error(f"Could not find admin1 geojson!")
        return None

    adm1_json = download_unzip_read_data(resource[0], read=True)
    if isinstance(adm1_json, type(None)):
        return None
    adm1_json.sort_values(by=["ADM1_PCODE"], inplace=True)

    if "un_boundaries" in scrapers_to_run:
        un_boundaries = update_un_boundaries(
            configuration,
            mapbox_auth,
        )
    if "boundaries" in scrapers_to_run:
        if not mapbox_auth:
            logger.error("No MapBox authorization provided")
            return None

        resource = find_resource(configuration["boundaries"]["dataset"], "geojson", kw="wrl_polbnda")
        if not resource:
            logger.error(f"Could not find admin0 geojson!")
            return None
        adm0_json = download_unzip_read_data(resource[0], read=True)
        if isinstance(adm0_json, type(None)):
            return None

        resource = find_resource(configuration["boundaries"]["dataset"], "geojson", kw="wrl_centroid")
        if not resource:
            logger.error(f"Could not find admin0 centroid geojson!")
            return None
        adm0_c_json = download_unzip_read_data(resource[0], read=True)
        if isinstance(adm0_c_json, type(None)):
            return None

        resource = find_resource(configuration["boundaries"]["dataset"], "geojson", kw="wrl_lakeresa")
        if not resource:
            logger.error(f"Could not find lakes geojson!")
            return None
        water_json = download_unzip_read_data(resource[0], read=True)
        if isinstance(water_json, type(None)):
            return None

        boundaries = update_boundaries(
            configuration,
            downloader,
            mapbox_auth,
            temp_folder,
            adm0_json,
            adm0_c_json,
            adm1_json,
            water_json,
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
