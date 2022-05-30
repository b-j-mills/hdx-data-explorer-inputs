import logging
from geopandas import GeoDataFrame

from scrapers.un_boundaries import update_un_boundaries
from scrapers.boundaries import update_boundaries
from scrapers.health_facilities import update_health_facilities
from scrapers.population import update_population
from scrapers.utilities.mapbox_functions import download_from_mapbox
from scrapers.utilities.helper_functions import find_resource, download_unzip_read_data
logger = logging.getLogger(__name__)


def get_indicators(
        configuration,
        downloader,
        temp_folder,
        mapbox_auth,
        data_source,
        update_tilesets,
        scrapers_to_run=None,
        countries=None,
        visualizations=None,
):

    if not scrapers_to_run:
        scrapers_to_run = ["boundaries", "health_facilities", "population"]

    if "un_boundaries" in scrapers_to_run:
        un_boundaries = update_un_boundaries(
            configuration,
            mapbox_auth,
        )

    adm1_countries = set()
    for viz in configuration["adm1"]:
        for country in configuration["adm1"][viz]:
            adm1_countries.add(country)
    adm1_countries = list(adm1_countries)
    adm1_countries.sort()

    if data_source == "hdx":
        resource = find_resource(configuration["boundaries"]["dataset"], "geojson", kw="polbnda_adm1")
        if not resource:
            return None
        adm1_json = download_unzip_read_data(resource[0], file_type="geojson", unzip=False, read=True)
    if data_source == "mapbox":
        adm1_json = download_from_mapbox(configuration["mapbox"]["global"]["polbnda_adm1"], mapbox_auth)
        if isinstance(adm1_json, type(None)):
            return None
        adm1_json = GeoDataFrame.from_features(adm1_json["features"])

    if "boundaries" in scrapers_to_run:
        boundaries = update_boundaries(
            configuration,
            downloader,
            mapbox_auth,
            temp_folder,
            adm1_json,
            data_source,
            update_tilesets,
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
