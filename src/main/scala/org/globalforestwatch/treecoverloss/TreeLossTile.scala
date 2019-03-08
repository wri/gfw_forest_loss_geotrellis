package org.globalforestwatch.treecoverloss

import geotrellis.raster.{CellGrid, CellType, Tile}

/** Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  *
  * @param lossYear
  * @param treeCoverDensity
  * @param biomass
  */
case class TreeLossTile(
  lossYear: Tile,
  treeCoverDensity: Tile,
  biomass: Tile
) extends CellGrid {
  def cellType: CellType = treeCoverDensity.cellType
  def cols: Int = treeCoverDensity.cols
  def rows: Int = treeCoverDensity.rows
}