# Tree Cover Loss Analysis

This project performs a polygonal summary on tree cover loss and intersecting layers for a given input feature using SPARK and Geotrellis.

## Analysis

Currently the following analysis are implemented

### Tree Cover Loss
A simple analysis which only looks at Tree Cover Loss, Tree Cover Density (2000 or 2010) and optionally Primary Forest.
Users can select one or many tree cover thresholds. Output will be a flat file, with one row per input feature and three cover density threshold.

This type of analysis only supports simple features as input. Best used together with the [ArcPY Client](https://github.com/wri/gfw_forest_loss_geotrellis_arcpy_client).

```sbt
sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain --analysis treecoverloss --feature_type simple --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix

``` 

### Annual Update
A complex analysis intersecting Tree Cover Loss data with more than 40 layers. This analysis is used for the GFU. Supported input features are GADM features only.
Output are Summary and Change tables for ISO, ADM1 and ADM2 areas.

```sbt
sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain --analysis annualupdate --feature_type gadm --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix

```

### Annual Update minimal
This analysis follows the same methodology as the annual update analysis above, just with fewer intersecting layers. 
It is used to compute statistics for the GFW country and user dashboards.

Supported input features are
-   GADM
-   Geostore
-   WDPA
-   Simple Feature

Output are Whitelist, Summary and Change tables for input features. For GADM, there will be seperate sets for ISO, ADM1 and ADM2 areas.
For GADM there will also be summary tables with one row per ISO, ADM1, ADM2 and Tree Cover Density, which is used for to prepare the Download Spreatsheets on the GFW country pages.
To produce final spreadsheets you will need to add another [post processing step](https://github.com/wri/write_country_stats).


```sbt
sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain --analysis annualupdate_minimal --feature_type gadm --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix
sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain --analysis annualupdate_minimal --feature_type wdpa --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix
sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain --analysis annualupdate_minimal --feature_type geostore --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix
sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain --analysis annualupdate_minimal --feature_type simple --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix

```

### Carbon Flux
Carbon Flux analysis is used to produce statistics for GFW climate topic pages.
It uses same approach as the annual update analysis, but with fewer and different input layers.
 


### Glad Alerts


## Inputs

USe Polygon Features encoded in TSV format. Geometries must be encoded in WKB. You can specify one or many input files using wildcards:

ex: 
-   `s3://bucket/prefix/gadm36_1_1.tsv`
-   `s3://bucket/prefix/geostore_*.tsv`


These features are intersected against rasters stored in a 10x10 degree grid schema on S3 (ex: `s3://gfw-files/2018_update/loss/${grid}.tif`)



## Output

Output are various `.csv` and `.json` files used on Global Forest Watch Website and API

CSV:
-   Tree Cover Loss Summary Statistics aggregated to Country level and Tree Cover Density Threshold for download from GFW country pages
-   Tree Cover Loss Summary Statistics aggregated to ADM1 level and Tree Cover Density Threshold for download from GFW country pages
-   Tree Cover Loss Summary Statistics aggregated to ADM2 level and Tree Cover Density Threshold for download from GFW country pages

JSON:
-   Tree Cover Loss Statistics aggregated to Country level and Tree Cover Density Threshold for injection into API
-   Tree Cover Loss Statistics aggregated to ADM1 level and Tree Cover Density Threshold for injection into API
-   Tree Cover Loss Statistics aggregated to ADM2 level and Tree Cover Density Threshold for injection into API

## Inventory

-   [`build.sbt`](build.sbt): Scala Build Tool build configuration file
-   [`.sbtopts`](.sbtopts): Command line options for SBT, including JVM parameters
-   [`project`](project): Additional configuration for SBT project, plugins, utility, versions
-   [`src/main/scala`](src/main/scala): Application and utility code
-   [`src/test/scala`](src/test/scala): Unit test files

## Spark Job Commands

### Local

For local testing input should be limited with `--limit` flag to minimize the time.

```sbt
sbt:geotrellis-wri> test:runMain org.globalforestwatch.summarystats.SummaryMain --features file:/Users/input/ten-by-ten-gadm36/wdpa__10N_010E.tsv --output file:/User/out/summary --limit 10
```

### EMR

Before running review `sbtlighter` configuration in `build.sbt`, `reload` SBT session if modified.

```sbt
sbt:geotrellis-wri> sparkCreateCluster

sbt:treecoverloss> sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain --features s3://gfw-files/2018_update/tsv/gadm36_1_1.csv --output s3://gfw-files/2018_update/results/summary --feature_type gadm --analysis annualupdate_minimal --tcl

sbt:treecoverloss> sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain  --features s3://gfw-files/2018_update/tsv/wdpa__*.tsv --output s3://gfw-files/2018_update/results/summary  --feature_type wdpa --analysis gladalerts --tcl --iso BRA
```