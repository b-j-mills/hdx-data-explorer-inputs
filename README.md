### Updater for HDX data explorers input data

This repo updates the national and subnational boundaries, pop density rasters, health facility dataset, and population dataset used in HDX data explorers such as the [COVID viewer](https://data.humdata.org/visualization/covid19-humanitarian-operations/), [Ukraine Data Explorer](https://data.humdata.org/visualization/ukraine-humanitarian-operations/), and others.

### Usage

The "render_pop_raster" script must be run within the QGIS project supplied with this script. Open the project, load the script, and add the list of ISO3 codes that you are trying to process to the *countries* parameter. Once the function completes the rendered images will have to be manually uploaded to MapBox for use.

Otherwise:

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-data-explorer-inputs** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE.
 
Other needed environment variables are: PREPREFIX, SCRAPERS_TO_RUN, COUNTRIES, VISUALIZATIONS, UPDATE_TILESETS, MAPBOX_AUTH, DATA_SOURCE.

### Process

#### UN boundaries

If the boundaries have been updated, put them in the **data_to_upload** folder. These are not public so cannot be uploaded to this repo. Running this function will update the source files on HDX or MapBox for use in the other scrapers.

#### Boundaries

COD administrative boundaries at admin 1 are downloaded, international boundaries are adjusted to match the UN boundaries, and they are converted to centroid. Both polygon and centroid admin 1 boundaries are updated in HDX, and individual tilesets used in the data explorers are updated.

Then bounding box geojsons for OCHA regions are generated for each visualization along with admin 1 info text documents. The admin 1 info docs are used in the scrapers that generate the input data for the explorers.

#### Health Facilities

HOTOSM health facility shapefiles are downloaded from HDX where available, spatially joined to the admin 1 boundaries, and summed by admin unit. The sums are joined to the admin 1 boundaries and saved as a csv. The resulting csv is uploaded to HDX as a resource in the [health facility dataset](https://data.humdata.org/dataset/admin-1-health-facilities-for-data-explorers/).

#### Population

UNFPA population data at admin 1 is read from HDX where available, joined to the admin 1 boundaries, and saved as a csv. The resulting csv is uploaded to HDX as a resource in the [population dataset](https://data.humdata.org/dataset/admin-1-population-statistics-for-data-explorers/).

