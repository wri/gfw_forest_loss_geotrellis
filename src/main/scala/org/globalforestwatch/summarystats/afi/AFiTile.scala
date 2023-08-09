package org.globalforestwatch.summarystats.afi

import geotrellis.raster.{CellGrid, CellType, IntCellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class AFiTile(
  treeCoverLoss: TreeCoverLoss#ITile,
  sbtnNaturalForest: SBTNNaturalForests#OptionalITile,
  negligibleRisk: NegligibleRisk#OptionalITile,
  gadmAdm0: GADMadm0#OptionalITile,
  gadmAdm1: GADMadm1#OptionalITile,
  gadmAdm2: GADMadm2#OptionalITile
) extends CellGrid[Int] {

  def cellType: CellType = treeCoverLoss.cellType

  def cols: Int = treeCoverLoss.cols

  def rows: Int = treeCoverLoss.rows
}
