package org.globalforestwatch.layers

case class MangroveBiomassExtent(grid: String)
    extends DBooleanLayer
    with OptionalDLayer {
  val uri: String =
    s"$basePath/mangrove_biomass/$grid.tif"

  def lookup(value: Double): Boolean = {
    if (value == 0) false
    else true
  }

}
