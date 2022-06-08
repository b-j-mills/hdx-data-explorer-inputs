import qgis.core
import processing
from os import mkdir
from os.path import join, exists
from urllib.request import urlretrieve


def main(countries):

    home_dir = QgsProject.instance().homePath()
    temp_folder = join(home_dir, "pop_density")
    if not exists(temp_folder):
        mkdir(temp_folder)

    for iso in countries:
        print(f"Processing population for {iso}")

        ftp_url = f"ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/{iso.upper()}/{iso.lower()}_ppp_2020.tif"
        r_file = join("temp_folder", f"{iso.lower()}_ppp_2020.tif")
        try:
            urlretrieve(ftp_url, r_file)
        except:
            print("Could not download file")

        r_layer = iface.addRasterLayer(r_file)
        r_layer.loadNamedStyle(join("../config", "yellow-orange-red style.qml"))

        # render
        extent = r_layer.extent()
        width = 3000
        height = int(round(r_layer.height() / r_layer.width() * 3000, 0))
        renderer = r_layer.renderer()
        provider = r_layer.dataProvider()

        pipe = QgsRasterPipe()
        pipe.set(provider.clone())
        pipe.set(renderer.clone())

        file_name = join(temp_folder, f"{iso.lower()}_ppp_2020_rendered.tif")
        file_writer = QgsRasterFileWriter(file_name)

        file_writer.writeRaster(pipe,
                                width,
                                height,
                                extent,
                                r_layer.crs())

        QgsProject.instance().removeMapLayer(r_layer)

        print(f"Finished processing {iso}")


if __name__ == "__main__":
    countries = []  # fill in country list here
    main(countries)
