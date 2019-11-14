package org.globalforestwatch.layers

case class AgcEmisYear(grid: String, model: String="standard") extends DoubleLayer with OptionalDLayer {
  val uri: String = s"$basePath/agc_emis_year/$model/$grid.tif"
}
