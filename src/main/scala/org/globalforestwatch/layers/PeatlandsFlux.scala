package org.globalforestwatch.layers

case class PeatlandsFlux(grid: String, model: String="standard")
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/peatlands_flux/$model/$grid.tif"
}
