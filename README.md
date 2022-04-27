### Updater for COVID visualization data

This repo updates the admin0, admin1, pop density, health facility, and population datasets used in HDX data explorers such as the  [COVID viewer](https://data.humdata.org/visualization/covid19-humanitarian-operations/), [Ukraine Data Explorer](https://data.humdata.org/visualization/ukraine-humanitarian-operations/), and others.

The "render_pop_raster" script must be run within the QGIS project supplied with this script. Open the project, load the script, and add the list of ISO3 codes that you are trying to process. Once that is complete they will have to be manually uploaded to MapBox for use.

run.py will run all three update scripts (boundaries, health facilities, and population).
