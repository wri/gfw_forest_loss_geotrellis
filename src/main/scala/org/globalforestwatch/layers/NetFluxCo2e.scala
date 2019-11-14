package org.globalforestwatch.layers

case class NetFluxCo2e(grid: String, model: String="standard")
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/net_flux_co2e/$model/$grid.tif"
}
