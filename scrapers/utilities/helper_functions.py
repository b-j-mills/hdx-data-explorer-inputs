import logging
import re
from glob import glob
from json import dump
from os import remove
from os.path import join, dirname, basename
from zipfile import ZipFile
from pandas import DataFrame, concat
from geopandas import read_file
from mapbox import Uploader
from time import sleep

from hdx.data.dataset import Dataset
from hdx.utilities.downloader import DownloadError
from hdx.data.hdxobject import HDXError

logger = logging.getLogger()


def find_resource(dataset_name, file_type, kw=None):
    try:
        dataset = Dataset.read_from_hdx(dataset_name)
    except HDXError:
        logger.warning(f"Could not find dataset {dataset_name}")
        return None

    if not dataset:
        logger.warning(f"Could not find dataset {dataset_name}")
        return None

    resources = dataset.get_resources()
    resource_list = []
    for r in resources:
        if r.get_file_type().lower() == file_type.lower():
            if kw:
                if bool(re.match(f".*{kw}.*", r["name"], re.IGNORECASE)):
                    resource_list.append(r)
            else:
                resource_list.append(r)

    if len(resource_list) == 0:
        logger.error(f"Could not find resource from {dataset_name}")
        return None

    return resource_list


def download_unzip_read_data(resource, file_type=None, unzip=False, read=False):
    try:
        _, resource_file = resource.download()
    except DownloadError:
        logger.error(f"Could not download resource")
        return None

    if unzip:
        temp_folder = join(dirname(resource_file), basename(resource_file).split(".")[0])
        with ZipFile(resource_file, "r") as z:
            z.extractall(temp_folder)
        out_files = glob(join(temp_folder, "**", f"*.{file_type}"), recursive=True)
    else:
        out_files = [resource_file]

    if len(out_files) == 0:
        logger.error(f"Did not find the file!")
        return None

    if read:
        if len(out_files) > 1:
            logger.error(f"Found more than one file for {resource['name']}")
            return None
        lyr = read_file(out_files[0])
        remove(out_files[0])
        return lyr

    return out_files


def replace_json(new_data, data_path):
    try:
        remove(data_path)
    except FileNotFoundError:
        logger.info("File does not exist - creating!")
    with open(data_path, "w") as f_open:
        dump(new_data, f_open)


def update_csv_resource(resource, downloader, new_adm1_data, countries):
    try:
        orig_data = downloader.download_tabular_rows_as_dicts(resource["url"])
    except DownloadError:
        logger.error(f"Could not download {resource['name']}")
        return None

    orig_data = DataFrame.from_dict(orig_data, orient="index")
    orig_data["ADM1_PCODE"] = orig_data.index
    orig_data.index.names = ["ADM1_PCODE"]
    orig_data.drop(orig_data.index[orig_data["alpha_3"].isin(countries)], inplace=True)

    adm1_data = concat([new_adm1_data, orig_data])

    return adm1_data


def drop_fields(df, keep_fields):
    df = df.drop(
        [f for f in df.columns if f not in keep_fields and f.lower() != "geometry"],
        axis=1,
    )
    return df


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
