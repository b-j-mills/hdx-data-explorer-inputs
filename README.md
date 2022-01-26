### Updater for COVID visualization data

This script updates the admin0, admin1, pop density, health facility, and population datasets used in the [COVID viewer](https://data.humdata.org/visualization/covid19-humanitarian-operations/).

The "render_pop_raster" script must be run within the QGIS project supplied with this script.

To run the admin boundary edit, update the project_configuration file with the ISO code (upper or lower case) and the action needed (add, remove, or replace).  If the country needs to be added to the admin1 boundaries, it must be added to the list of ISO codes in the configuration.  If a country should be removed from the admin1 boundaries, its ISO code must be removed from the HRPs in the configuration. If a country should be removed from the admin0 boundaries, its ISO code must be removed from the GHOs in the configuration.

The update health facilities script only runs on the HRP country list in the configuration. Add or remove countries and be sure to also add dataset names on HDX.
