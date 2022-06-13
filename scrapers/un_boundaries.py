import logging
from glob import glob
from os.path import join

from hdx.data.hdxobject import HDXError
from scrapers.utilities.hdx_functions import find_resource
from scrapers.utilities.mapbox_functions import replace_mapbox_dataset

logger = logging.getLogger()


def update_un_boundaries(
    configuration,
    mapbox_auth,
    dest="HDX",
):
    logger.info(f"Uploading datasets to {dest}")

    mapids = configuration["mapbox"]["global"]
    for dataset_name in mapids:
        logger.info(f"Processing {dataset_name}")

        in_files = glob(join("data_to_upload", f"*{dataset_name}*.geojson"))
        if len(in_files) != 1:
            logger.error("Found the wrong number of files - skipping!")
            continue

        if dest.lower() == "hdx":
            # update in HDX
            resource = find_resource(
                configuration["boundaries"]["dataset"], "geojson", kw=dataset_name
            )
            if not resource:
                continue

            resource[0].set_file_to_upload(in_files[0])

            try:
                resource[0].update_in_hdx()
            except HDXError:
                logger.exception("Could not update resource")
                continue

        if dest.lower() == "mapbox":
            dataset_mapid = mapids[dataset_name]
            replace_mapbox_dataset(dataset_mapid, mapbox_auth, path_to_upload=in_files[0])
            logger.info(f"Finished processing {dataset_name}")

    return
