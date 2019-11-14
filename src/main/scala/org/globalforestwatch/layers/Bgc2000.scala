package org.globalforestwatch.layers

case class Bgc2000(grid: String, model: String="standard") extends DoubleLayer with OptionalDLayer {
  val uri: String = s"$basePath/bgc_2000/$model/$grid.tif"
}
