import logging
import geopandas
import json
import qgis.core
import processing
from glob import glob
from shutil import copyfile
from os import remove, rename
from os.path import join, basename, dirname, exists
from urllib.request import urlretrieve
from datetime import date
from yaml import load


os.chdir(QgsProject.instance().homePath())
logging.basicConfig(filename="errors.log", level = "INFO", force = True)


def add_vector_layer(file_path):
    layer = iface.addVectorLayer(file_path,basename(file_path).split(".")[0],"ogr")
    return layer


def add_raster_layer(file_path):
    layer = iface.addRasterLayer(file_path,basename(file_path).split(".")[0])
    return layer


def add_fields(layer, fields):
    layer_provider = layer.dataProvider()
    for f in fields:
        if not f in layer.fields().names():
            layer_provider.addAttributes([QgsField(f, fields[f])])
            layer.updateFields()
    return layer


def remove_fields(layer, fields):
    layer_provider = layer.dataProvider()
    del_fields = []
    for f in layer.fields().names():
        if not f in fields:
            del_fields.append(layer.fields().indexFromName(f))
    del_fields.sort(reverse=True)
    for i in del_fields:
        layer_provider.deleteAttributes([i])
    layer.updateFields()
    return layer


def simplify(in_path, out_path):
    processing.run("native:simplifygeometries", 
    {"INPUT": in_path, "METHOD": 0, "TOLERANCE": 0.001, "OUTPUT": out_path})


def make_centroid(in_path, out_path):
    processing.run("native:centroids",
    {"INPUT": in_path, "ALL_PARTS": 0, "OUTPUT": out_path})


def remove_country_rows(layer, isos, invert):
    iso_field = [i for i in layer.fields().names() if i in ["alpha_3", "ISO_3"]]
    try:
        iso_field = iso_field[0]
    except IndexError:
        return None
    with edit(layer):
        if invert == 1:
            ids = [feat.id() for feat in layer.getFeatures() if feat[iso_field] not in isos]
        else:
            ids = [feat.id() for feat in layer.getFeatures() if feat[iso_field] in isos]
        lf_layer.deleteFeatures(ids)
    return layer


def remove_crs(in_path):
    with open(in_path, 'r') as f_open:
        data = json.load(f_open)
    new_data = {}
    for element in data:
        if not element == "crs":
            new_data[element] = data[element]
    remove(in_path)
    with open(in_path, 'w') as f_open:
        json.dump(new_data, f_open)


def main():
    today = date.today().strftime("%Y_%m_%d")
    logging.info(f"##### covid-mapbox, date: {today} ####")
    with open(join("config", "project_configuration.yml"), "r") as project_yml:
        configuration = load(project_yml)
    
    try:
        iso = configuration["iso"][0].upper()
    except (IndexError, KeyError):
        logging.error(f"Cannot run function without ISO code!")
        return None
    
    try:
        action = configuration["action"][0].lower()
    except (IndexError, KeyError):
        logging.error(f"Cannot run function without action!")
        return None
    
    if action not in ["add", "remove", "replace"]:
        logging.error(f"Action {action} not in allowed set of actions!")
        return None
    
    try:
        HRPs = configuration["HRPs"]
    except KeyError:
        logging.error("HRP list not found in config file!")
        return None
    
    # remove all layers from map
    layers = QgsProject.instance().mapLayers()
    for l in layers:
        QgsProject.instance().removeMapLayer(l)
    
    # copy files from latest to archive and remove editing HRP file
    latest_files = glob(join("Geoprocessing", "latest", "*", "*.geojson"))
    archive_files = [lf.split(".")[0].replace("latest","archive") + f"_{today}." + lf.split(".")[1] for lf in latest_files]
    for i in range(len(latest_files)):
        try:
            lf = latest_files[i]
            af = archive_files[i]
            copyfile(lf, af)
        except IOError:
            logging.error(f"Unable to copy file: {lf}")
            return None
        if basename(lf)[:4] != "hrp_" and dirname(lf) == "Geoprocessing/latest/adm1":
            remove(lf)
    
    # make copy of admin1 files for editing
    latest_adm1 = glob(join("Geoprocessing", "latest", "adm1", "*.geojson"))
    for lf in latest_adm1:
        try:
            copyfile(lf, lf.replace("hrp_", f"hrp{len(HRPs)}_"))
        except:
            logging.error(f"Unable to copy file: {lf}")
            return None
    
    logging.info("Made copies of all files")
    
    if action in ["replace", "remove"]:
        latest_adm1 = glob(join("Geoprocessing", "latest", "adm1", "*.geojson"))
        latest_adm0 = glob(join("Geoprocessing", "latest", "adm0", "*.geojson"))
        
        for lf in latest_adm1:
            if basename(lf)[:4] == "hrp_" and action == "remove":
                continue
            lf_layer = add_vector_layer(lf)
            lf_layer = remove_country_rows(lf_layer, HRPs, 1)
            lf_layer = remove_country_rows(lf_layer, [iso], 0)
            if lf_layer is None:
                logging.error(f"Country field not found in {basename(lf)}!")
                return None
            QgsProject.instance().removeMapLayer(lf_layer)
        
        for lf in latest_adm0:
            lf_layer = add_vector_layer(lf)
            lf_layer = remove_country_rows(lf_layer, [iso], 0)
            if lf_layer is None:
                logging.error(f"Country field not found in {basename(lf)}!")
                return None
            QgsProject.instance().removeMapLayer(lf_layer)
                
        logging.info(f"Removed country {iso} from all jsons")
    
    if action in ["add", "replace"]:
        
        # add admin0 boundaries to JSONs in all cases
        logging.info(f"Adding boundaries from {iso} to admin0")
        
        # find file
        new_admin0 = glob(join("Geoprocessing", "new", "adm0", iso.lower(), "*.shp"))
        try:
            new_admin0 = new_admin0[0]
        except IndexError:
            logging.error(f"Cannot find admin0 file!")
            return None
        
        # simplify
        adm0_simp = join("Geoprocessing", "new", "adm0", iso.lower(), f"{iso.lower()}_simp.shp")
        simplify(new_admin0, adm0_simp)
        
        # Add needed fields
        adm0_layer_simp = add_vector_layer(adm0_simp)
        fields = {"ISO_3": QVariant.String, "Terr_ID": QVariant.Int, "Terr_Name": QVariant.String}
        adm0_layer_simp = add_fields(adm0_layer_simp, fields)
        
        # calculate fields
        layer_provider = adm0_layer_simp.dataProvider()
        adm0_name = [i for i in adm0_layer_simp.fields().names() if "ADM0" in i and not "ALT" in i and not "PCODE" in i and not "REF" in i]
        try:
            adm0_name = adm0_name[0]
        except IndexError:
            logging.error("Country field not found!")
            return None
        
        countryname = NULL
        adm0_layer_simp.startEditing()
        for f in adm0_layer_simp.getFeatures():
            id=f.id()
            countryname = f.attributes()[adm0_layer_simp.fields().indexFromName("ADM0_REF")]
            if countryname == NULL:
                countryname = f.attributes()[adm0_layer_simp.fields().indexFromName(adm0_name)]
            if countryname == NULL:
                logging.info("Country name not found in adm0 attribute table")
            
            attr_value={adm0_layer_simp.fields().indexFromName("Terr_Name"):countryname,
            adm0_layer_simp.fields().indexFromName("ISO_3"):iso, 
            adm0_layer_simp.fields().indexFromName("Terr_ID"):0}
            layer_provider.changeAttributeValues({id:attr_value})
        
        adm0_layer_simp.commitChanges()
        
        # convert to centroid
        adm0_cent = join("Geoprocessing", "new", "adm0", iso.lower(), f"{iso.lower()}_cent.shp")
        make_centroid(adm0_simp, adm0_cent)
        adm0_layer_cent = add_vector_layer(adm0_cent)
        
        #Merge with existing JSONs
        latest_adm0 = glob(join("Geoprocessing", "latest", "adm0", "*.geojson"))
        
        try:
            latest_polbnda_file = [i for i in latest_adm0 if "polbnda" in basename(i)][0]
        except IndexError:
            logging.error(f"Cannot find copied admin0 boundary!")
            return None
        
        latest_polbnda_rename = latest_polbnda_file.split(".")[0] + "_edit." + latest_polbnda_file.split(".")[1]
        rename(latest_polbnda_file, latest_polbnda_rename)
        latest_polbnda = add_vector_layer(latest_polbnda_rename)
        
        try:
            latest_centroid_file = [i for i in latest_adm0 if "centroid" in basename(i)][0]
        except IndexError:
            logging.error(f"Cannot find copied admin0 centroid!")
            return None
        
        latest_centroid_rename = latest_centroid_file.split(".")[0] + "_edit." + latest_centroid_file.split(".")[1]
        rename(latest_centroid_file, latest_centroid_rename)
        latest_centroid = add_vector_layer(latest_centroid_rename)
        
        processing.run("native:mergevectorlayers", 
        {"LAYERS": [latest_polbnda, adm0_layer_simp], "CRS": "EPSG:4326", "OUTPUT": latest_polbnda_file})
        
        processing.run("native:mergevectorlayers", 
        {"LAYERS": [latest_centroid, adm0_layer_cent], "CRS": "EPSG:4326", "OUTPUT": latest_centroid_file})
        
        # remove unnecessary layers
        QgsProject.instance().removeMapLayer(latest_polbnda)
        QgsProject.instance().removeMapLayer(latest_centroid)
        QgsProject.instance().removeMapLayer(adm0_layer_simp)
        QgsProject.instance().removeMapLayer(adm0_layer_cent)
        
        # delete renamed files
        remove(latest_polbnda_rename)
        remove(latest_centroid_rename)
        
        # add final layers to map
        adm0_layer_bnd = add_vector_layer(latest_polbnda_file)
        adm0_layer_cent = add_vector_layer(latest_centroid_file)
        
        # delete unneeded fields
        adm0_layer_bnd = remove_fields(adm0_layer_bnd, fields)
        adm0_layer_cent = remove_fields(adm0_layer_cent, fields)
        
        # remove CRS entry from centroid json
        QgsProject.instance().removeMapLayer(adm0_layer_cent)
        remove_crs(latest_centroid_file)
        adm0_layer_cent = add_vector_layer(latest_centroid_file)
        
        logging.info("Added adm0 features to final jsons")
        
        
        # download raster if iso is in HRPs in config file
        if iso in HRPs:
            
            if action == "add":
                
                # download raster if iso is in HRPs in config file
                ftp_url = f"ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/{iso}/{iso.lower()}_ppp_2020.tif"
                r_file = join("Geoprocessing", "new", "pop_density", f"{iso.lower()}_ppp_2020.tif")
                urlretrieve(ftp_url, r_file)

                # add raster to project
                r_layer = add_raster_layer(r_file)
                
                # load style
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
                
                # remove raster layer
                QgsProject.instance().removeMapLayer(r_layer)
                logging.info("Finished processing raster layer")
            
            
            # add admin1 boundaries to JSONs
            logging.info(f"Adding boundaries from {iso} to admin1")
            
            # find file
            new_admin1 = glob(join("Geoprocessing", "new", "adm1", iso.lower(), "*.shp"))
            try:
                new_admin1 = new_admin1[0]
            except IndexError:
                logging.error(f"Cannot find admin1 file!")
                return None
                
            # simplify
            adm1_simp = join("Geoprocessing", "new", "adm1", iso.lower(), f"{iso.lower()}_simp.shp")
            simplify(new_admin1, adm1_simp)
            
            # Add needed fields
            adm1_layer_simp = add_vector_layer(adm1_simp)
            fields = {"ADM1_PCODE": QVariant.String, "ADM1_REF": QVariant.String, 
            "ADM0_PCODE": QVariant.String, "alpha_3": QVariant.String, "ADM0_REF": QVariant.String}
            adm1_layer_simp = add_fields(adm1_layer_simp, fields)
            
            # calculate fields adlpha_3, ADM0_REF, and ADM1_REF
            layer_provider = adm1_layer_simp.dataProvider()
            adm0_name = [i for i in adm1_layer_simp.fields().names() if "ADM0" in i and not "ALT" in i and not "PCODE" in i and not "REF" in i]
            try:
                adm0_name = adm0_name[0]
            except IndexError:
                logging.error("Country field not found!")
                return None
            
            adm1_name = [i for i in adm1_layer_simp.fields().names() if "ADM1" in i and not "ALT" in i and not "PCODE" in i and not "REF" in i]
            try:
                adm1_name = adm1_name[0]
            except IndexError:
                logging.error("Admin 1 name field not found!")
                return None
            
            countryname = NULL
            provname = NULL
            adm1_layer_simp.startEditing()
            for f in adm1_layer_simp.getFeatures():
                id=f.id()
                countryname = f.attributes()[adm1_layer_simp.fields().indexFromName("ADM0_REF")]
                if countryname == NULL:
                    countryname = f.attributes()[adm1_layer_simp.fields().indexFromName(adm0_name)]
                if countryname == NULL:
                    logging.info(f"Country name not found in adm0 attribute table for id {id}")
                provname = f.attributes()[adm1_layer_simp.fields().indexFromName("ADM1_REF")]
                if provname == NULL:
                    provname = f.attributes()[adm1_layer_simp.fields().indexFromName(adm1_name)]
                if provname == NULL:
                    logging.info(f"Admin 1 name not found in adm1 attribute table for id {id}")
                attr_value={adm1_layer_simp.fields().indexFromName("ADM0_REF"):countryname,
                adm1_layer_simp.fields().indexFromName("ADM1_REF"):provname,
                adm1_layer_simp.fields().indexFromName("alpha_3"):iso}
                layer_provider.changeAttributeValues({id:attr_value})
            
            adm1_layer_simp.commitChanges()
            
            # convert to centroid
            adm1_cent = join("Geoprocessing", "new", "adm1", iso.lower(), f"{iso.lower()}_cent.shp")
            make_centroid(adm1_simp, adm1_cent)
            adm1_layer_cent = add_vector_layer(adm1_cent)
            
            #Merge with existing JSONs
            latest_adm1 = glob(join("Geoprocessing", "latest", "adm1", "*.geojson"))
            latest_polbnda_files = [i for i in latest_adm1 if "polbnda" in basename(i)]
            latest_centroid_files = [i for i in latest_adm1 if "centroid" in basename(i)]
            
            for latest_polbnda_file in latest_polbnda_files:
                latest_polbnda_rename = latest_polbnda_file.split(".")[0] + "_edit." + latest_polbnda_file.split(".")[1]
                rename(latest_polbnda_file, latest_polbnda_rename)
                latest_polbnda = add_vector_layer(latest_polbnda_rename)
                processing.run("native:mergevectorlayers", 
                {"LAYERS": [latest_polbnda, adm1_layer_simp], "CRS": "EPSG:4326", "OUTPUT": latest_polbnda_file})
                QgsProject.instance().removeMapLayer(latest_polbnda)
                remove(latest_polbnda_rename)
                adm1_layer_bnd = add_vector_layer(latest_polbnda_file)
                adm1_layer_bnd = remove_fields(adm1_layer_bnd, fields)
                QgsProject.instance().removeMapLayer(adm1_layer_bnd)
                
            for latest_centroid_file in latest_centroid_files:
                latest_centroid_rename = latest_centroid_file.split(".")[0] + "_edit." + latest_centroid_file.split(".")[1]
                rename(latest_centroid_file, latest_centroid_rename)
                latest_centroid = add_vector_layer(latest_centroid_rename)
                processing.run("native:mergevectorlayers", 
                {"LAYERS": [latest_centroid, adm1_layer_cent], "CRS": "EPSG:4326", "OUTPUT": latest_centroid_file})
                QgsProject.instance().removeMapLayer(latest_centroid)
                remove(latest_centroid_rename)
                adm1_layer_cent_latest = add_vector_layer(latest_centroid_file)
                adm1_layer_cent_latest = remove_fields(adm1_layer_cent_latest, fields)
                QgsProject.instance().removeMapLayer(adm1_layer_cent_latest)
                remove_crs(latest_centroid_file)
                
                
            # remove unnecessary layers
            QgsProject.instance().removeMapLayer(adm1_layer_simp)
            QgsProject.instance().removeMapLayer(adm1_layer_cent)
            
            # add back final layers
            adm1_layer_bnd1 = add_vector_layer(latest_polbnda_files[0])
            adm1_layer_cent1 = add_vector_layer(latest_centroid_files[0])
            adm1_layer_bnd2 = add_vector_layer(latest_polbnda_files[1])
            adm1_layer_cent2 = add_vector_layer(latest_centroid_files[1])
            
            
            logging.info("Added adm1 features to final jsons")
    
    # create admin1 lookup for COVID viz yml
    latest_adm1 = glob(join("Geoprocessing", "latest", "adm1", "hrp[0-9]*polbnda*.geojson"))
    try:
        latest_adm1 = latest_adm1[0]
    except IndexError:
        logging.error(f"Cannot find admin1 boundary!")
        return None
    adm1_layer = add_vector_layer(latest_adm1)
    
    attributes = list()
    for f in adm1_layer.getFeatures():
        attributes.append({
        "ADM0_REF": f.attributes()[adm1_layer.fields().indexFromName("ADM0_REF")],
        "alpha_3": f.attributes()[adm1_layer.fields().indexFromName("alpha_3")], 
        "ADM0_PCODE": f.attributes()[adm1_layer.fields().indexFromName("ADM0_PCODE")], 
        "ADM1_PCODE": f.attributes()[adm1_layer.fields().indexFromName("ADM1_PCODE")],
        "ADM1_REF": f.attributes()[adm1_layer.fields().indexFromName("ADM1_REF")],
        })
    attributes = sorted(attributes, key = lambda i: i['ADM1_PCODE'])
    
    with open(join("config","adm1_attributes.txt"), "w") as f:
        for item in attributes:
            f.write("%s\n" % str(item).replace("'",""))
    
    return "Processed"


if __name__ == "__main__":
    message = main()
    if not message is None:
        print(message)

