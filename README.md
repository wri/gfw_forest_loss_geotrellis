# Tree Cover Loss Analysis

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d2ccce68bf9f462bae80f5c576a28b24)](https://www.codacy.com/manual/thomas-maschler/gfw_forest_loss_geotrellis?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=wri/gfw_forest_loss_geotrellis&amp;utm_campaign=Badge_Grade)

This project performs a polygonal summary on tree cover loss and intersecting layers for a given input feature using SPARK and Geotrellis.

## Analysis

Currently the following analysis are implemented

*   Tree Cover Loss (for ArcPy client)
*   Annual Update minimal
*   Carbon Flux Full Standard Model
*   Carbon Flux Sensitivity Analysis
*   Glad Alerts
*   Viirs/ MODIS Fire Alerts
*   Forest Change Diagnostic
*   GFW Pro Dashboard
*   Integrated Alerts

### Tree Cover Loss

A simple analysis which only looks at tree cover loss, tree cover density (2000 or 2010), aboveground biomass,
gross GHG emissions, gross carbon removals, and net GHG flux. Aboveground carbon, belowground carbon, and soil carbon stocks in 2000
and annual emissions from aboveground biomass loss (old emissions model) can be optionally output.
Users can select one or many tree cover thresholds. Output will be a flat file, with one row per input feature and tree cover density threshold.
Optional contextual analysis layers include plantations, humid tropical primary forests, global peat, drivers of tree cover loss, and tree cover loss due to fires.
Contextual layers are lazily analyzed, meaning that they are not loaded into the analysis if the user does not request them (to save memory).
The emissions outputs (annual and total) are from the forest carbon flux model (Harris et al. 2021 Nature Climate Change) [forest carbon flux model](https://github.com/wri/carbon-budget).
Outputs also include carbon removals and carbon net flux (total only) (Harris et al. 2021). 
Emissions, removals, and net flux are reported for (> tree cover density 2000 threshold OR Hansen gain OR mangrove extent NOT pre-2000 plantations) 
because the model results
include not just pixels above a certain tree cover density threshold, but also Hansen gain pixels.
Other outputs from this analysis (loss, gain, biomass, carbon pools, etc.) use the simple tree cover density threshold. 

This type of analysis only supports simple features as input. Best used together with the [ArcPY Client](https://github.com/wri/gfw_forest_loss_geotrellis_arcpy_client).

```sbt
test:runMain org.globalforestwatch.summarystats.SummaryMain treecoverloss --feature_type feature --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix --tcd 2000 --threshold 0 --threshold 30 --contextual_layer is__gfw_plantations --contextual_layer tsc_drivers__class --carbon_pools
```

### Annual Update minimal

An analysis intersecting Tree Cover Loss data with a number of layers.
It is used to compute statistics for the GFW country and user dashboards, including carbon flux outputs (emissions, removals, net flux).

Supported input features are

*   GADM
*   Geostore
*   WDPA
*   Simple Feature

Output are Whitelist, Summary and Change tables for input features. For GADM, there will be seperate sets for ISO, ADM1 and ADM2 areas.
For GADM there will also be summary tables with one row per ISO, ADM1, ADM2 and Tree Cover Density, which is used for to prepare the Download Spreatsheets on the GFW country pages.
To produce final spreadsheets you will need to add another [post processing step](https://github.com/wri/write_country_stats).

```sbt
test:runMain org.globalforestwatch.summarystats.SummaryMain annualupdate_minimal --feature_type gadm --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix
test:runMain org.globalforestwatch.summarystats.SummaryMain annualupdate_minimal --feature_type wdpa --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix
test:runMain org.globalforestwatch.summarystats.SummaryMain annualupdate_minimal --feature_type geostore --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix
test:runMain org.globalforestwatch.summarystats.SummaryMain annualupdate_minimal --feature_type feature --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix
```

### Carbon Flux Full Standard Model

Carbon Flux (full standard model) analysis is used to produce detailed statistics for carbon flux research, with additional model-specific contextual layers.
It uses the same approach as the annual update analysis, but with all stock and flux outputs from the [forest carbon flux model](https://github.com/wri/carbon-budget). 
It also analyzes several contextual layers that are unique to the carbon flux model and does not include many contextual layers used in the annual update minimal analysis. 
It currently only works with GADM features.

```sbt
test:runMain org.globalforestwatch.summarystats.SummaryMain carbonflux --feature_type gadm --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix
```

### Carbon Flux Sensitivity Analysis

Carbon Flux analysis is used to produce statistics from sensitivity analysis runs of the [forest carbon flux model](https://github.com/wri/carbon-budget).
It uses the same approach as the Carbon Flux Full Standard Model, but with fewer model outputs analyzed. 
The sensitivity analysis argument needs to match the folder names in the input tile folders (e.g., maxgain, biomass_swap).
To run this model with the standard flux model output for an analysis using fewer flux model outputs, use `standard` as the `sensitivity_type` argument.
It currently only works with GADM features. 

```sbt
test:runMain org.globalforestwatch.summarystats.SummaryMain carbon_sensitivity --feature_type gadm --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix --sensitivity_type sensitivity_analysis_type
test:runMain org.globalforestwatch.summarystats.SummaryMain carbon_sensitivity --feature_type gadm --tcl --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix --sensitivity_type standard
```

### Glad Alerts

Glad alert analysis computes whitelist, summary, daily and weekly change data for given input features and intersects areas with the same contextual layers as in annual update minimal.
It is used to update the country and user dashboards for the GFW website.

Users can select, if they want to run the full analysis, or only look at change data. Computing only change data makes sense, if neither input features, nor contextual layers have changed, but only glad alerts.
In that case, only the daily and weekly change tables will be updated. 

Supported input features are

*   GADM
*   Geostore
*   WDPA
*   Simple Feature

```sbt
test:runMain org.globalforestwatch.summarystats.SummaryMain gladalerts --feature_type gadm --glad --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix [--change_only]
test:runMain org.globalforestwatch.summarystats.SummaryMain gladalerts --feature_type wdpa --glad --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix [--change_only]
test:runMain org.globalforestwatch.summarystats.SummaryMain gladalerts --feature_type geostore --glad --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix [--change_only]
test:runMain org.globalforestwatch.summarystats.SummaryMain gladalerts --feature_type feature --glad --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix [--change_only]
```

### Viirs/ MODIS Fire Alerts

Viirs/ MODIS Fire alert analysis computes whitelist, daily and weekly change data for given input features and intersects areas with the same contextual layers as in glad alerts. 
It also enriches all fire data with intersecting contextual information.
It is used to update the country and user dashboards for the GFW website.

Supported input features are

*   GADM
*   Geostore
*   WDPA
*   Simple Feature

```sbt
test:runMain org.globalforestwatch.summarystats.SummaryMain firealerts --fire_alert_type MODIS/VIIRS --fire_alert_source s3://bucket/prefix/file.tsv --feature_type gadm --glad --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix [--change_only]
test:runMain org.globalforestwatch.summarystats.SummaryMain firealerts --fire_alert_type MODIS/VIIRS --fire_alert_source s3://bucket/prefix/file.tsv --feature_type wdpa --glad --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix [--change_only]
test:runMain org.globalforestwatch.summarystats.SummaryMain firealerts --fire_alert_type MODIS/VIIRS --fire_alert_source s3://bucket/prefix/file.tsv --feature_type geostore --glad --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix [--change_only]
test:runMain org.globalforestwatch.summarystats.SummaryMain firealerts --fire_alert_type MODIS/VIIRS --fire_alert_source s3://bucket/prefix/file.tsv --feature_type feature --glad --features s3://bucket/prefix/file.tsv --output s3://bucket/prefix [--change_only]
```
### Forest Change Diagnostic

Forest Change Diagnostic computes forest loss and fire alerts for an input set of
lists of geometries. It is used to compute statistics for lists of a GFW Pro
customer. It requires the `firealerts` options and optionally takes any `feature`
options. The only supported input feature is `gfwpro`. Automatically turns on the
`--split_features` option.

```sbt
test:runMain org.globalforestwatch.summarystats.SummaryMain forest_change_diagnostic --feature_type gfwpro --features s3://bucket/prefix/file.tsv --fire_alert_source s3://bucket/prefix/file.tsv --output s3://bucket/prefix
```

There are two test input files available in the source tree, so you can run a sample
forest change diagnostic locally without s3 via a command like:

```sbt
test:runMain org.globalforestwatch.summarystats.SummaryMain forest_change_diagnostic --split_features --feature_type gfwpro --features src/test/resources/palm-oil-32.tsv --fire_alert_type modis --fire_alert_source src/test/resources/sample-fire-alert.tsv --output testout
```

### GFW Pro Dashboard

GFW Pro Dashboard computes summary statistics for the GFW Pro Dashboard. It
optionally takes `firealert` options and `feature` options.

Supported input features are:

*   GADM
*   Geostore
*   WDPA
*   Simple Feature
*   GFW Pro


### Integrated Alerts

Integrated Alerts computes a combination of deforestation alerts based on GLAD-L,
GLAD-S2, and RADD systems. It is used to compute these alerts for GFW. It optionally
takes any `feature`, `gadm`, or `wdpa` options.

Supported input features are

*   GADM
*   Geostore
*   WDPA
*   Simple Feature
*   GFW Pro


## Inputs

Use Polygon Features encoded in TSV format. Geometries must be encoded in WKB. You can specify one or many input files using wildcards:

ex: 

*   `s3://bucket/prefix/gadm36_1_1.tsv`
*   `s3://bucket/prefix/geostore_*.tsv`

Make sure features are sufficiently small to assure a well balanced partition size and workload.
Larger features should be split into smaller features, prior to running the analysis. 
Also make sure, that features do not overlap with tile boundaries (we use 10x10 degree tiles). 
For best performance, intersect input features with a 1x1 degree grid.
If you are not sure how to best approach this, simply use the [ArcPY Client](https://github.com/wri/gfw_forest_loss_geotrellis_arcpy_client)
Alternatively, use the `--split_features` opton.

## Options

The following options are supported:

|Option           |Type  |Analysis or Feature Type | Description                                                                                                                                             |
|-----------------|------|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
|features         |string|all (required)           | URI of features in TSV format                                                                                                                           |
|output           |string|all (required)           | URI of output dir for CSV files                                                                                                                         |
|feature_type     |string|all (required)           | Feature type: one of 'gadm', 'wdpa', 'geostore', 'feature', or 'gfwpro'                                                                                 |
|limit            |int   |all                      | Limit number of records processed                                                                                                                       |
|iso_first        |string|`gadm` or `wdpa` features| Filter by first letter of ISO code                                                                                                                      |
|iso_start        |string|`gadm` or `wdpa` features| Filter by ISO code larger than or equal to given value                                                                                                  |
|iso_end          |string|`gadm` or `wdpa` features| Filter by ISO code smaller than given value                                                                                                             |
|iso              |string|`gadm` or `wdpa` features| Filter by country ISO code                                                                                                                              |
|admin1           |string|`gadm` features          | Filter by country Admin1 code                                                                                                                           |
|admin2           |string|`gadm` features          | Filter by country Admin2 code                                                                                                                           |
|id_start         |int   |`feature` analysis       | Filter by IDs greater than or equal to given value                                                                                                      |
|id_end           |int   |`feature` analysis       | Filter by IDs less than or equal to given value                                                                                                         |
|wdpa_status      |string|`wdpa` features          | Filter by WDPA Status                                                                                                                                   |
|iucn_cat         |string|`wdpa` features          | Filter by IUCS Category                                                                                                                                 |
|tcd              |int   |`treecoverloss` analysis | Select tree cover density year                                                                                                                          |
|threshold        |int   |`treecoverloss` analysis | Treecover threshold to apply (multiple)                                                                                                                 |
|contextual_layer |string|`treecoverloss` analysis | Include (multiple) selected contextual layers: `is__umd_regional_primary_forest_2001`, `is__gfw_plantations`, `is__global_peat`, `tsc_drivers__class`, `is__tree_cover_loss_fire` |
|carbon_pools     |flag  |`treecoverloss` analysis | Optionally calculate stock sums for multiple carbon pools in 2000 (aboveground, belowground, soil)                                                      |
|tcl              |flag  |all                      | Filter input feature by TCL tile extent, requires boolean `tcl` field in input feature class                                                            |
|glad             |flag  |all                      | Filter input feature by GLAD tile extent, requires boolean `glad` field in input feature class                                                          |
|change_only      |flag  |all except `treecover`   | Process change only                                                                                                                                     |
|sensitivity_type |string|`carbon_sensitivity`     | Select carbon sensitivity model                                                                                                                         |
|fire_alert_type  |string|`firealerts` | Select Fire alert type                                                                                                                                  |
|fire_alert_source|string|`firealerts` | URI of fire alert TSV file                                                                                                                              |
|overwrite        |flag  |all                      | Overwrite output location if already existing                                                                                                           |
|split_features   |flag  |all                      | Split input features along 1x1 degree grid                                                                                                              |
|no_output_path_suffix|flag |all                   | Do not autogenerate output path suffix at runtime                                                                                                       |
|intermediate_list_source|flag |`forest_change_diagnostic` analysis | URI of intermediate list results in TSV format                                                                                                          |
|contextual_feature_type|flag |`gfwpro_dashboard` analysis | type of contextual feature                                                                                                                              |
|contextual_feature_url|flag |`gfwpro_dashboard` analysis | URI of contextual feature in TSV format                                                                                                                 |


## Inventory

*   [`build.sbt`](build.sbt): Scala Build Tool build configuration file
*   [`.sbtopts`](.sbtopts): Command line options for SBT, including JVM parameters
*   [`project`](project): Additional configuration for SBT project, plugins, utility, versions
*   [`src/main/scala`](src/main/scala): Application and utility code
*   [`src/test/scala`](src/test/scala): Unit test files

## Spark Job Commands

### Local

```sbt
sbt:geotrellis-wri> test:runMain org.globalforestwatch.summarystats.SummaryMain annualupdate_minimal --features file:/Users/input/ten-by-ten-gadm36/wdpa__10N_010E.tsv --output file:/User/out/summary
```
    
### EMR

Before running review `sbtlighter` configuration in `build.sbt`, `reload` SBT session if modified.

```sbt
sbt:geotrellis-wri> sparkCreateCluster
sbt:treecoverloss> sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain annualupdate_minimal --features s3://gfw-files/2018_update/tsv/gadm36_1_1.csv --output s3://gfw-files/2018_update/results/summary --feature_type gadm --tcl
sbt:treecoverloss> sparkSubmitMain org.globalforestwatch.summarystats.SummaryMain gladalerts --features s3://gfw-files/2018_update/tsv/wdpa__*.tsv --output s3://gfw-files/2018_update/results/summary  --feature_type wdpa --tcl --iso BRA
```
