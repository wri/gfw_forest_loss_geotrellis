package org.globalforestwatch.layers

case class PeatlandsFlux(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/peatlands_flux/$grid.tif"
}
