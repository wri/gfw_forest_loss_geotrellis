package org.globalforestwatch.layers

case class TropicLatitudeExtent(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/tropic_latitude_extent/$grid.tif"
}
