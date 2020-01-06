package org.globalforestwatch.layers

case class AgcEmisYear(grid: String, model: String = "standard") extends FloatLayer with OptionalFLayer {
  val uri: String = s"$basePath/agc_emis_year/$model/$grid.tif"
}
