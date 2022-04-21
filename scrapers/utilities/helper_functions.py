import logging
import re
from glob import glob
from json import dump, load
from os import remove
from os.path import join, basename
from zipfile import ZipFile
from pandas import DataFrame, concat

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


def download_unzip_data(downloader, resource, file_type, folder):
    try:
        resource_zip = downloader.download_file(resource["url"], path=folder)
    except DownloadError:
        logger.error(f"Could not download resource")
        return None

    temp_folder = basename(resource_zip)

    with ZipFile(resource_zip, "r") as z:
        z.extractall(join(temp_folder, "unzipped"))

    out_files = glob(join(temp_folder, "unzipped", "**", f"*.{file_type}"), recursive=True)

    if len(out_files) == 0:
        logger.error(f"Did not find the {file_type} in the zip")
        return None

    return out_files


def replace_json(new_data, data_path):
    remove(data_path)
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
