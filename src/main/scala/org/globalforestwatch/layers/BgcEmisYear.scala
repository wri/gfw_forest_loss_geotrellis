package org.globalforestwatch.layers

case class BgcEmisYear(grid: String) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"$basePath/bgc_emis_year/$grid.tif"
}
