package usbuildings

import geotrellis.raster.{CellGrid, CellType, Tile}

case class TreeLossTile(
  loss_year: Tile,
  tcd: Tile,
  co2: Tile
) extends CellGrid {
  def cellType: CellType = tcd.cellType
  def cols: Int = tcd.cols
  def rows: Int = tcd.rows
}