import logging
from glob import glob
from os.path import join
from json import load
from scrapers.utilities.mapbox_functions import (
    replace_mapbox_tileset,
    replace_mapbox_dataset,
)

logger = logging.getLogger()


def update_un_boundaries(
        configuration,
        mapbox_auth,
        temp_folder,
):
    dataset_mapids = configuration["mapbox"]["global"]["datasets"]
    tileset_mapids = configuration["mapbox"]["global"]["tilesets"]

    dataset_names = [n for n in configuration["mapbox"]["global"]["tilesets"]]

    for dataset_name in dataset_names:
        logger.info(f"Processing {dataset_name}")

        dataset_mapid = dataset_mapids[dataset_name]
        tileset_mapid = tileset_mapids[dataset_name]
        tileset_name = f"global_{dataset_name.replace('-','_')}"

        in_files = glob(join("data_to_upload", f"*{dataset_name.replace('-','_')}*.geojson"))
        if len(in_files) != 1:
            logger.error("Found the wrong number of files - skipping!")
            continue
        with open(in_files[0]) as f:
            in_json = load(f)
        replace_mapbox_dataset(dataset_mapid, mapbox_auth, in_json)
        replace_mapbox_tileset(tileset_mapid, mapbox_auth, tileset_name, path_to_upload=in_files[0])
        logger.info(f"Finished processing {dataset_name}")

    return dataset_names
