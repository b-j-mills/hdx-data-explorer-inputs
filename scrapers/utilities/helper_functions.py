import logging
from glob import glob
from json import dump, load
from os import remove
from os.path import join, basename
from zipfile import ZipFile
from pandas import DataFrame, concat

from hdx.data.dataset import Dataset
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger()


def find_resource(dataset_name, file_type, kw=None):
    dataset = Dataset.read_from_hdx(dataset_name)

    if not dataset:
        logger.error(f"Could not find dataset")
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
        logger.error(f"Could not find resource")
        return None

    return resource


def unzip_data(downloaded_resource, file_type):
    temp_folder = basename(downloaded_resource)

    with ZipFile(downloaded_resource, "r") as z:
        z.extractall(join(temp_folder, "unzipped"))

    out_files = glob(join(temp_folder, "unzipped", f"*.{file_type}"))

    return out_files


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

