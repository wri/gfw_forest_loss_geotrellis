package org.globalforestwatch.layers

case class Agc2000(grid: String, model: String="standard") extends DoubleLayer with OptionalDLayer {
  val uri: String = s"$basePath/agc_2000/$model/$grid.tif"
}
