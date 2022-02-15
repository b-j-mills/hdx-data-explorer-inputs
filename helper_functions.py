import logging
from datetime import date
from glob import glob
from json import dump, load
from os import remove
from os.path import join
from shutil import copyfile
from zipfile import ZipFile

from hdx.data.dataset import Dataset
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger()


def copy_files_to_archive(data_type):
    today = date.today().strftime("%Y_%m_%d")
    latest_files = glob(join("Geoprocessing", "latest", data_type, "*"))
    archive_files = [
        lf.split(".")[0].replace("latest", "archive") + f"_{today}." + lf.split(".")[1]
        for lf in latest_files
    ]
    for i in range(len(latest_files)):
        lf = latest_files[i]
        af = archive_files[i]
        try:
            copyfile(lf, af)
        except IOError:
            logger.error(f"Unable to copy file: {lf}")
            continue


def retrieve_data(iso, dataset_name, data_category, file_type, kw=None):
    dataset = Dataset.read_from_hdx(dataset_name)

    if not dataset:
        logger.error(f"Could not find dataset for {iso}")
        return None

    resources = dataset.get_resources()
    resource = None
    for r in resources:
        if r.get_file_type() == file_type:
            if kw:
                if kw.lower() in r["name"].lower():
                    resource = r
            else:
                resource = r
        if resource:
            break

    if not resource:
        logger.error(f"Could not find {file_type} for {iso}")
        return None

    working_folder = join("Geoprocessing", "new", data_category)

    try:
        downloaded_resource = resource.download(working_folder)
    except DownloadError:
        logger.error(f"Could not download shapefile for {iso}")
        return None

    return downloaded_resource[1]


def unzip_data(iso, downloaded_resource, data_category, file_type):
    working_folder = join("Geoprocessing", "new", data_category)

    with ZipFile(downloaded_resource, "r") as z:
        z.extractall(join(working_folder, iso))

    out_file = glob(join(working_folder, iso, f"*.{file_type}"))

    if len(out_file) != 1:
        logger.error(f"Found {len(out_file)} files for {iso}")
        return None

    return out_file[0]


def replace_json(new_data, data_path):
    remove(data_path)
    with open(data_path, "w") as f_open:
        dump(new_data, f_open)


def remove_crs(data_path):
    with open(data_path, "r") as f_open:
        data = load(f_open)
    new_data = {}
    for element in data:
        if not element == "crs":
            new_data[element] = data[element]
    replace_json(new_data, data_path)
