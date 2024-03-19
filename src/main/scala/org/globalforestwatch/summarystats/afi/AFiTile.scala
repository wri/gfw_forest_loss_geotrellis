package org.globalforestwatch.summarystats.afi

import geotrellis.raster.{CellGrid, CellType}
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
  gadmAdm0: GadmAdm0#OptionalITile,
  gadmAdm1: GadmAdm1#OptionalITile,
  gadmAdm2: GadmAdm2#OptionalITile,
  jrcForestCover: JRCForestCover#OptionalITile
) extends CellGrid[Int] {

  def cellType: CellType = treeCoverLoss.cellType

  def cols: Int = treeCoverLoss.cols

  def rows: Int = treeCoverLoss.rows
}
