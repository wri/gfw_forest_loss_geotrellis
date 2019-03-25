package org.globalforestwatch.treecoverloss

import geotrellis.raster.{CellGrid, CellType, Tile}

/** Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  *
  * @param loss Tree Cover Loss Tile
  * @param gain Tree Cover Gain Tile
  * @param tcd2000 Tree Cover Density 2000 Tile
  * @param tcd2010 Tree Cover Density 2010 Tile
  * @param co2Pixel Tons of CO2 per Pixel Tile
  * @param gadm36 GADM v3.6 (admin level 2) Tile
  *
  */
case class TreeLossTile(
  loss: Tile,
  gain: Tile,
  tcd2000: Tile,
 // tcd2010: Tile,
  co2Pixel: Option[Tile],
  gadm36: Option[Tile]
) extends CellGrid {
  def cellType: CellType = loss.cellType
  def cols: Int = loss.cols
  def rows: Int = loss.rows
}