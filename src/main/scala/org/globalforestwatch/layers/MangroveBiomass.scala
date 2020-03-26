package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class MangroveBiomass(gridTile: GridTile) extends DoubleLayer with OptionalDLayer {
  val uri: String =
    s"$basePath/mangrove_biomass/${gridTile.tileId}..tif"
}
