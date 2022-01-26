import logging
import qgis.core
import processing
from os import chdir
from os.path import join, basename, exists
from urllib.request import urlretrieve
from datetime import date
from yaml import load


chdir(QgsProject.instance().homePath())
logging.basicConfig(filename="errors.log", level="INFO", force=True)


def main():
    today = date.today().strftime("%Y_%m_%d")
    logging.info(f"##### covid-mapbox, date: {today} ####")
    with open(join("config", "project_configuration.yml"), "r") as project_yml:
        configuration = load(project_yml)

    params = configuration.get("isos")
    if not params:
        logging.error(f"Cannot run function without ISO codes and actions!")
        return
    for iso in params:
        action = params.get(iso)

        if not action.lower() == "add":
            continue

        logging.info(f"Processing raster layer for {iso}")

        r_file = join("Geoprocessing", "new", "pop_density", f"{iso.lower()}_ppp_2020.tif")
        if not exists(r_file):
            ftp_url = f"ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/{iso.upper()}/{iso.lower()}_ppp_2020.tif"
            urlretrieve(ftp_url, r_file)

        r_layer = iface.addRasterLayer(r_file, basename(r_file).split(".")[0])

        r_layer.loadNamedStyle(join("config","yellow-orange-red style.qml"))

        # render
        extent = r_layer.extent()
        width = 3000
        height = int(round(r_layer.height() / r_layer.width() * 3000, 0))
        renderer = r_layer.renderer()
        provider=r_layer.dataProvider()

        pipe = QgsRasterPipe()
        pipe.set(provider.clone())
        pipe.set(renderer.clone())

        file_name = join("Geoprocessing", "latest", "pop_density", f"{iso.lower()}_ppp_2020.tif")
        file_writer = QgsRasterFileWriter(file_name)

        file_writer.writeRaster(pipe,
                                width,
                                height,
                                extent,
                                r_layer.crs())

        QgsProject.instance().removeMapLayer(r_layer)
        logging.info(f"Finished processing raster layer for {iso}")


if __name__ == "__main__":
    main()

