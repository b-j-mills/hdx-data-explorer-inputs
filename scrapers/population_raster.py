import qgis.core
import processing
from os import listdir
from os.path import join


def main():
    home_dir = QgsProject.instance().homePath()

    for mapper in ["covid", "covid-arabic"]:
        new_folder = join(home_dir, "Geoprocessing", mapper, "new", "pop_density")
        latest_folder = join(home_dir, "Geoprocessing", mapper, "latest", "pop_density")

        r_list = [r for r in listdir(new_folder)]
        for r in r_list:

            print(f"Processing {r} layer for mapper {mapper}")

            r_file = join(new_folder, r)

            r_layer = iface.addRasterLayer(r_file, r.split(".")[0])

            r_layer.loadNamedStyle(join("../config", "yellow-orange-red style.qml"))

            # render
            extent = r_layer.extent()
            width = 3000
            height = int(round(r_layer.height() / r_layer.width() * 3000, 0))
            renderer = r_layer.renderer()
            provider=r_layer.dataProvider()

            pipe = QgsRasterPipe()
            pipe.set(provider.clone())
            pipe.set(renderer.clone())

            file_name = join(latest_folder, r)
            file_writer = QgsRasterFileWriter(file_name)

            file_writer.writeRaster(pipe,
                                    width,
                                    height,
                                    extent,
                                    r_layer.crs())

            QgsProject.instance().removeMapLayer(r_layer)

            print(f"Finished processing {r} layer")


if __name__ == "__main__":
    main()

