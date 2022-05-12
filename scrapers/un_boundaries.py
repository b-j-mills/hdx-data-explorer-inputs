import logging
from glob import glob
from os.path import join
from geojson import load
from geopandas import GeoDataFrame
from scrapers.utilities.mapbox_functions import (
    replace_mapbox_dataset,
)

logger = logging.getLogger()


def update_un_boundaries(
        configuration,
        mapbox_auth,
):
    mapids = configuration["mapbox"]["global"]

    for dataset_name in mapids:
        logger.info(f"Processing {dataset_name}")

        dataset_mapid = mapids[dataset_name]
        in_files = glob(join("data_to_upload", f"*{dataset_name.replace('-','_')}*.geojson"))
        if len(in_files) != 1:
            logger.error("Found the wrong number of files - skipping!")
            continue
        with open(in_files[0]) as f:
            in_json = load(f)
        in_json = GeoDataFrame.from_features(in_json["features"])
        replace_mapbox_dataset(dataset_mapid, mapbox_auth, json_to_upload=in_json)
        logger.info(f"Finished processing {dataset_name}")

    return mapids
