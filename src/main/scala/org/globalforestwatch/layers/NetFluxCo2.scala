package org.globalforestwatch.layers

case class NetFluxCo2(grid: String) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"$basePath/net_flux_co2/$grid.tif"
}
