package org.globalforestwatch.summarystats.treecoverloss

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class TreeLossTile(loss: TreeCoverLoss#ITile,
                        gain: TreeCoverGain#ITile,
                        tcd2000: TreeCoverDensity2000#ITile,
                        tcd2010: TreeCoverDensity2010#ITile,
                        biomass: BiomassPerHectar#OptionalDTile,
                        primaryForest: PrimaryForest#OptionalITile,
                        plantationsBool: PlantationsBool#OptionalITile)

  extends CellGrid[Int] {

  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
