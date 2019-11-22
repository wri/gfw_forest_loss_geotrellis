package org.globalforestwatch.layers

case class BgcEmisYear(grid: String, model: String = "standard") extends FloatLayer with OptionalFLayer {
  val uri: String = s"$basePath/bgc_emis_year/$model/$grid.tif"
}
