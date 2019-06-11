package org.globalforestwatch.layers

case class NetFluxCo2(grid: String) extends DoubleLayer with OptionalDLayer {
  val uri: String = s"$basePath/net_flux_co2/$grid.tif"
}
