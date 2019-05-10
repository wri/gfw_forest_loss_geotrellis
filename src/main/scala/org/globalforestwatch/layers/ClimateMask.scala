package org.globalforestwatch.layers

case class ClimateMask(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/climate/climate_mask/climate_mask_$grid.tif"
}
