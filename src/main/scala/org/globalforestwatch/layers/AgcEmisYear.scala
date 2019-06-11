package org.globalforestwatch.layers

case class AgcEmisYear(grid: String) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"$basePath/agc_emis_year/$grid.tif"
}
