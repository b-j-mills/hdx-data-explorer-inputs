import logging
from json import dump
from os import remove

logger = logging.getLogger()


def replace_json(new_data, data_path):
    try:
        remove(data_path)
    except FileNotFoundError:
        logger.info("File does not exist - creating!")
    with open(data_path, "w") as f_open:
        dump(new_data, f_open)


def drop_fields(df, keep_fields):
    df = df.drop(
        [f for f in df.columns if f not in keep_fields and f.lower() != "geometry"],
        axis=1,
    )
    return df
