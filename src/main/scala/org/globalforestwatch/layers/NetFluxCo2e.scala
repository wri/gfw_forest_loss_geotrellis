package org.globalforestwatch.layers

case class NetFluxCo2e(grid: String) extends DoubleLayer with OptionalDLayer {
  val uri: String = s"$basePath/net_flux_co2e/$grid.tif"
}
