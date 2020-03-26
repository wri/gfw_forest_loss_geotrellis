package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class MangroveBiomassExtent(gridTile: GridTile)
    extends DBooleanLayer
    with OptionalDLayer {
  val uri: String =
    s"$basePath/mangrove_biomass/${gridTile.tileId}..tif"

  def lookup(value: Double): Boolean = {
    if (value == 0) false
    else true
  }

}
