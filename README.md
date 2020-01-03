# Tree Cover Loss Analysis

This project performs a polygonal summary on tree cover loss and intersecting layers for a given input feature.

## Inputs

Features encoded in TSV format (ex: `s3://gfw-files/2018_update/tsv/gadm36_1_1.csv`).

These features are intersected against rasters stored in a 10x10 degree grid schema on S3 (ex: `s3://gfw-files/2018_update/loss/${grid}.tif`)

## Output

Output are various `.csv` and `.json` files used on Global Forest Watch Website and API

CSV:
-  Tree Cover Loss Summary Statistics aggregated to Country level and Tree Cover Density Threshold for download from GFW country pages
-  Tree Cover Loss Summary Statistics aggregated to ADM1 level and Tree Cover Density Threshold for download from GFW country pages
-  Tree Cover Loss Summary Statistics aggregated to ADM2 level and Tree Cover Density Threshold for download from GFW country pages

JSON:
-  Tree Cover Loss Statistics aggregated to Country level and Tree Cover Density Threshold for injection into API
-  Tree Cover Loss Statistics aggregated to ADM1 level and Tree Cover Density Threshold for injection into API
-  Tree Cover Loss Statistics aggregated to ADM2 level and Tree Cover Density Threshold for injection into API

## Inventory

-  [`build.sbt`](build.sbt): Scala Build Tool build configuration file
-  [`.sbtopts`](.sbtopts): Command line options for SBT, including JVM parameters
-  [`project`](project): Additional configuration for SBT project, plugins, utility, versions
-  [`src/main/scala`](src/main/scala): Application and utility code
-  [`src/test/scala`](src/test/scala): Unit test files

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