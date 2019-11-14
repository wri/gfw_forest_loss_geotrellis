package org.globalforestwatch.layers

case class BgcEmisYear(grid: String, model: String="standard") extends DoubleLayer with OptionalDLayer {
  val uri: String = s"$basePath/bgc_emis_year/$model/$grid.tif"
}
