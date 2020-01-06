package org.globalforestwatch.layers

case class Agc2000(grid: String) extends DoubleLayer with OptionalDLayer {
  val uri: String = s"$basePath/agc_2000/$grid.tif"
}
