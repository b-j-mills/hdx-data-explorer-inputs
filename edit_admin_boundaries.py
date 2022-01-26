import logging
from glob import glob
from os import remove, rename
from os.path import join, basename, dirname
from helper_functions import copy_files_to_archive, remove_crs
from geopandas import read_file, GeoDataFrame
from pandas import isna

from hdx.api.configuration import Configuration
from hdx.facades.simple import facade
from hdx.utilities.easy_logging import setup_logging
from hdx.location.country import Country

setup_logging()
logger = logging.getLogger()


def main():
    logger.info(f"Editing admin boundaries")

    configuration = Configuration.read()
    HRPs = configuration.get("HRPs")
    GHOs = configuration.get("GHOs")
    params = configuration.get("isos")

    copy_files_to_archive("adm0")
    copy_files_to_archive("adm1")
    
    latest_adm1 = glob(join("Geoprocessing", "latest", "adm1", "*.geojson"))
    for lf in latest_adm1:
        rename(lf, lf.replace(basename(lf)[:6], f"hrp{len(HRPs)}_"))
    
    logger.info("Made copies of all files")

    for iso in params:
        logger.info(f"Processing {iso}")

        action = params.get(iso)
        if not action:
            logger.error(f"Could not find action for {iso}")
            continue

        action = action.lower()

        # remove any country boundaries that have been removed from GHO or HRP lists or that are marked "replace"
        latest_files = glob(join("Geoprocessing", "latest", "adm*", "*.geojson"))

        for lf in latest_files:
            adm_lyr = read_file(lf)
            adm0_field = "ISO_3"
            if "alpha_3" in adm_lyr.columns:
                adm0_field = "alpha_3"
            if "adm0" in dirname(lf):
                adm_lyr.drop(adm_lyr.index[~adm_lyr[adm0_field].isin(GHOs)], inplace=True)
            if "adm1" in dirname(lf):
                adm_lyr.drop(adm_lyr.index[~adm_lyr[adm0_field].isin(HRPs)], inplace=True)
            if action == "replace":
                adm_lyr.drop(adm_lyr.index[adm_lyr[adm0_field] == iso], inplace=True)
            remove(lf)
            adm_lyr.to_file(lf, driver="GeoJSON")

        logger.info(f"Removed countries needing replacement or not in HRP or GHO lists")
    
        if action in ["add", "replace"]:

            logger.info(f"Adding boundaries for {iso}")

            fields = {
                "adm0": ["ISO_3", "Terr_ID", "Terr_Name"],
                "adm1": ["ADM1_PCODE", "ADM1_REF", "ADM0_PCODE", "alpha_3", "ADM0_REF"]
                      }
            new_bounds = glob(join("Geoprocessing", "new", "adm*", iso.lower(), "*.shp"))

            for new_bound in new_bounds:
                # simplify
                adm_level = dirname(new_bound).split("/")[-2]
                logger.info(f"Processing {adm_level}")

                new_lyr = read_file(new_bound)
                new_lyr.geometry = new_lyr.geometry.simplify(tolerance=0.001, preserve_topology=True)

                # calculate fields
                adm_fields = fields.get(adm_level)
                for adm_field in adm_fields:
                    if adm_field == "Terr_ID":
                        new_lyr[adm_field] = 0
                    if adm_field in ["ISO_3", "alpha_3"]:
                        new_lyr[adm_field] = iso.upper()
                    if adm_field in ["Terr_Name", "ADM0_REF"]:
                        new_lyr[adm_field] = Country.get_country_name_from_iso3(iso)
                    if adm_field == "ADM1_REF":
                        adm1_name = ["ADM1_EN"]
                        if adm1_name[0] not in list(new_lyr.columns):
                            adm1_name = [i for i in new_lyr.columns if
                                         "ADM1" in i and "ALT" not in i and "PCODE" not in i and "REF" not in i]
                        try:
                            adm1_name = adm1_name[0]
                        except IndexError:
                            logger.error(f"Admin 1 name field not found for {iso}!")
                            return
                        if adm_field not in list(new_lyr.columns):
                            new_lyr[adm_field] = new_lyr[adm1_name]
                        else:
                            new_lyr.loc[isna(new_lyr["ADM1_REF"]), "ADM1_REF"] = new_lyr[adm1_name]

                # delete fields
                new_lyr.drop(
                    [f for f in new_lyr.columns.tolist() if f not in adm_fields and f.lower() != "geometry"],
                    axis=1,
                    inplace=True
                )

                # save edited file
                edited_bound = join(dirname(new_bound), f"{iso.lower()}_polbnda.geojson")
                new_lyr.to_file(edited_bound, driver="GeoJSON")

                # convert to centroid
                new_cent = GeoDataFrame(new_lyr.representative_point())
                new_cent.rename(columns={0: "geometry"}, inplace=True)
                new_cent[adm_fields] = new_lyr[adm_fields]
                new_cent.set_geometry("geometry", inplace=True)
                cent_bound = join(dirname(new_bound), f"{iso.lower()}_centroid.geojson")
                new_cent.to_file(cent_bound, driver="GeoJSON")

                # Merge with existing JSONs
                latest_polbndas = glob(join("Geoprocessing", "latest", adm_level, "*polbnda*.geojson"))
                latest_centroids = glob(join("Geoprocessing", "latest", adm_level, "*centroid*.geojson"))

                for latest_polbnda in latest_polbndas:
                    latest_lyr = read_file(latest_polbnda)
                    merged_lyr = latest_lyr.append(new_lyr)
                    remove(latest_polbnda)
                    merged_lyr.to_file(latest_polbnda, driver="GeoJSON")

                for latest_centroid in latest_centroids:
                    latest_lyr = read_file(latest_centroid)
                    merged_lyr = latest_lyr.append(new_cent)
                    remove(latest_centroid)
                    merged_lyr.to_file(latest_centroid, driver="GeoJSON")
                    remove_crs(latest_centroid)

                logger.info(f"Done processing {adm_level}")

    # create admin1 lookup for COVID viz yml
    latest_adm1 = glob(join("Geoprocessing", "latest", "adm1", "hrp[0-9]*polbnda*.geojson"))
    try:
        latest_adm1 = latest_adm1[0]
    except IndexError:
        logger.error(f"Cannot find final admin1 boundary!")
        return

    latest_lyr = read_file(latest_adm1)
    attributes = list()
    for index, row in latest_lyr.iterrows():
        attributes.append({
            "country": row["ADM0_REF"],
            "iso3": row["alpha_3"],
            "pcode": row["ADM1_PCODE"],
            "name": row["ADM1_REF"],
        })
    attributes = sorted(attributes, key=lambda i: (i["country"], i["name"]))

    with open(join("config", "adm1_attributes.txt"), "w") as f:
        for row in attributes:
            if "," in row["name"]:
                row["name"] = '"' + row["name"] + '"'
            if "'" in row["name"]:
                row["name"] = row["name"].replace("'", "|")
            f.write("- %s\n" % str(row).replace("'", "").replace("|", "'"))

    logger.info("Created admin1 lookup")

    return


if __name__ == "__main__":
    facade(
        main,
        hdx_read_only=True,
        user_agent="test",
        project_config_yaml=join("config", "project_configuration.yml"),
    )