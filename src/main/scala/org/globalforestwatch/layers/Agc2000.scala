package org.globalforestwatch.layers

case class Agc2000(grid: String, model: String = "standard") extends FloatLayer with OptionalFLayer {
  val uri: String = s"$basePath/agc_2000/$model/$grid.tif"
}
