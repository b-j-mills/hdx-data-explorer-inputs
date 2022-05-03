import logging
from os.path import join
from mapbox import Uploader, Datasets
from time import sleep

logger = logging.getLogger()


def download_from_mapbox(mapid, mapbox_auth):
    datasets = Datasets(access_token=mapbox_auth)
    response = datasets.list_features(dataset=mapid)
    if response.status_code != 200:
        logger.error(f"Could not retrieve dataset {mapid}: error {response.status_code}")
        return None
    feature_list = response.json()
    return feature_list


def update_mapbox(mapid, file_to_upload, mapbox_auth, temp_folder, name):
    service = Uploader(access_token=mapbox_auth)
    saved_file = join(temp_folder, "file_to_upload.geojson")
    file_to_upload.to_file(saved_file, driver="GeoJSON")
    with open(saved_file, 'rb') as src:
        upload_resp = service.upload(src, mapid, name=name)
    if upload_resp.status_code == 422:
        for i in range(5):
            sleep(5)
            with open(saved_file, 'rb') as src:
                upload_resp = service.upload(src, mapid, name=name)
            if upload_resp.status_code != 422:
                break
    if upload_resp.status_code == 422:
        logger.error(f"Could not upload {name}")
        return None
    return mapid
