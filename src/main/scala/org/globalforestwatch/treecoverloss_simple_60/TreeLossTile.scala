package org.globalforestwatch.treecoverloss_simple_60

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class TreeLossTile(loss: TreeCoverLoss#ITile,
                        tcd2010: TreeCoverDensity2010_60#ITile)
    extends CellGrid {
  def cellType: CellType = loss.cellType
  def cols: Int = loss.cols
  def rows: Int = loss.rows
}
