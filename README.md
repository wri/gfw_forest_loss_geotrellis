# GeoTrellis Polygonal Summary Demo

This project demonstrates generating polygonal summary for a feature data set over high resolution raster.

## Inputs

Features used are building footprints from: https://github.com/Microsoft/USBuildingFootprints

> This dataset contains 125,192,184 computer generated building footprints in all 50 US states.

These features are read directly from zipped GeoJSON files.

The rasters used are 1/3 arc-second NED GeoTiffs hosted at: `s3://azavea-datahub/raw/ned-13arcsec-geotiff/`
The files are gridded and named by their northing and easting degree (ex: `imgn36w092_13.tif`).

The rasters are sampled directly from that location.

### Alternative Input
Alternative input feature is demonstrated as CSV file, with one column containing WKT string of the geometry.
This file was converted from original GeoJSON using included [utility code](https://github.com/echeipesh/geotrellis-wri-workshop/blob/1c003730d75e2b756465666d78d9cb7f13a0dee9/src/main/scala/usbuildings/Util.scala#L28-L48)

`s3://geotrellis-workshop/wri/vermont-buildings.csv`

## Output

Output is a set of `.csv` files with geometry WKT, feature id and min/max NED values under that geometry.

## Inventory

[`build.sbt`](build.sbt): Scala Build Tool build configuration file
[`.sbtopts`](.sbtopts): Command line options for SBT, including JVM parameters
[`project`](project): Additional configuration for SBT project, plugins, utility, versions
[`src/main/scala`](src/main/scala): Application and utility code

## Spark Job Commands

### Local

```
sbt:geotrellis-wri> test:runMain usbuildings.BuildingElevationMain --features file:/User/Vermont.geojson --output file:/User/output-dir --sample 0.01

sbt:geotrellis-wri> test:runMain usbuildings.BuildingElevationCsvMain --features file:/User/vermont-buildings.csv --output file:/User/output-dir --limit 100
```

### EMR

Before running review `sbtlighter` configuration in `build.sbt`, `reload` SBT session if modified.

```
sbt:geotrellis-wri> sparkCreateCluster

sbt:geotrellis-wri> sparkSubmitMain usbuildings.BuildingElevationMain --all-features --output s3://bucket/buildings/output
```