# GeoTrellis Polygonal Summary Demo

This project demonstrates generating polygonal summary for a feature data set over high resolution raster.

## Inputs

Features encoded in TSV format (ex: `s3://gfw2-data/alerts-tsv/country-pages/ten-by-ten-gadm36/wdpa__10N_010E.tsv`).

These features are intersected against rasters stored on:


  - `s3://gfw2-data/forest_change/hansen_2018/${grid}.tif`
  - `s3://gfw2-data/forest_cover/2000_treecover/Hansen_GFC2014_treecover2000_${grid}.tif`
  - `s3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/${grid}_t_aboveground_biomass_ha_2000.tif`

## Output

Output is a set of `.csv` files with custom polygonal summary per feature `country,area_type,admin1,admin2` combination per year present in the input features.

Example:
```
country,area_type,admin1,admin2,year,tcd_mean,biomass_sum
AZE,wdpa,7,6,6,0.0,1.0
TCD,wdpa,16,1,12,15.9963456577816,NaN
RUS,wdpa,36,21,6,60.15094339622642,NaN
CMR,wdpa,3,4,18,72.09787234042552,49230.0
TCD,wdpa,16,3,18,15.07417837151001,NaN
CMR,wdpa,10,1,5,90.27906976744185,12759.0
CMR,wdpa,7,2,9,16.95,1797.0
CAF,wdpa,13,1,13,54.89473684210526,26006.0
IRN,wdpa,15,1,11,0.0,80.0
KAZ,wdpa,10,12,11,36.51162790697674,NaN
GAB,wdpa,7,3,8,76.28571428571429,1656.0
CMR,wdpa,5,2,16,66.78260869565217,21585.0
GNQ,wdpa,4,3,10,91.6470588235294,4883.0
RUS,wdpa,80,4,5,27.65991451403085,NaN
```

## Inventory

- [`build.sbt`](build.sbt): Scala Build Tool build configuration file
- [`.sbtopts`](.sbtopts): Command line options for SBT, including JVM parameters
- [`project`](project): Additional configuration for SBT project, plugins, utility, versions
- [`src/main/scala`](src/main/scala): Application and utility code
- [`src/test/scala`](src/test/scala): Unit test files

## Spark Job Commands

### Local

For local testing input should be limited with `--limit` flag to minimize the time.

```
sbt:geotrellis-wri> test:runMain org.globalforestwatch.treecoverloss.TreeLossSummaryMain --features file:/Users/input/ten-by-ten-gadm36/wdpa__10N_010E.tsv --output file:/User/out/summary --limit 10
```

### EMR

Before running review `sbtlighter` configuration in `build.sbt`, `reload` SBT session if modified.

```
sbt:geotrellis-wri> sparkCreateCluster

sbt:treecoverloss> sparkSubmitMain org.globalforestwatch.treecoverloss.TreeLossSummaryMain --features --features s3://gfw-files/2018_update/tsv/gadm36_1_1.csv --output s3://gfw-files/2018_update/results/summary

sbt:treecoverloss> sparkSubmitMain org.globalforestwatch.treecoverloss.TreeLossSummaryMain --features --features s3://gfw-files/2018_update/tsv/gadm36_1_1.csv --output s3://gfw-files/2018_update/results/summary --iso BRA

sbt:treecoverloss> sparkSubmitMain org.globalforestwatch.treecoverloss.TreeLossSummaryMain --features s3://gfw2-data/alerts-tsv/country-pages/ten-by-ten-gadm36/wdpa__*.tsv --output s3://geotrellis-test/wri/out/gobal-summary
```