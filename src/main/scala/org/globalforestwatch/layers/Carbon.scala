package org.globalforestwatch.layers

case class Carbon(grid: String) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"$basePath/co2_pixel/$grid.tif"
}
