package org.globalforestwatch.layers

case class PrimaryForest(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/umd_regional_primary_forest_2001/v201901/raster/epsg-4326/$grid.tif"
}
