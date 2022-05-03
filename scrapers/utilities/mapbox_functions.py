import logging
from os.path import join
from mapbox import Uploader, Datasets
from time import sleep

logger = logging.getLogger()


def replace_mapbox_dataset(mapid, mapbox_auth, json_to_upload):
    datasets = Datasets(access_token=mapbox_auth)
    response = datasets.list_features(dataset=mapid)
    if response.status_code != 200:
        logger.error(f"Could not retrieve dataset {mapid}: error {response.status_code}")
        return None
    feature_list = response.json()
    if len(feature_list["features"]) > 0:
        for feature in feature_list["features"]:
            fid = feature["id"]
            response = datasets.delete_feature(mapid, fid)
            if response.status_code != 204:
                logger.warning(f"Feature {fid} may not have been deleted from {mapid}: error {response.status_code}")
    for i in range(len(json_to_upload["features"])):
        fid = str(i+1)
        response = datasets.update_feature(mapid, fid, json_to_upload["features"][i])
        if response.status_code != 200:
            logger.error(f"Could not update feature {i} in dataset {mapid}: error {response.status_code}")
    return None


def download_from_mapbox(mapid, mapbox_auth):
    datasets = Datasets(access_token=mapbox_auth)
    feature_list = {"type": "FeatureCollection",
                    "features": []}
    nogos_in_a_row = 0
    for i in range(1000):
        response = datasets.read_feature(dataset=mapid, fid=str(i))
        if response.status_code == 200:
            nogos_in_a_row = 0
            feature_list["features"].append(response.json())
        else:
            nogos_in_a_row += 1
        if nogos_in_a_row >= 10:
            break

    return feature_list


def replace_mapbox_tileset(mapid, mapbox_auth, name, path_to_upload=None, json_to_upload=None, temp_folder=None):
    service = Uploader(access_token=mapbox_auth)
    if not isinstance(path_to_upload, type(None)):
        saved_file = path_to_upload
    if not isinstance(json_to_upload, type(None)):
        if temp_folder is None:
            logger.error("Need to provide temp folder path")
            return None
        saved_file = join(temp_folder, "file_to_upload.geojson")
        json_to_upload.to_file(saved_file, driver="GeoJSON")
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
