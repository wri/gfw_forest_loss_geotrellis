package org.globalforestwatch.summarystats.forest_change_diagnostic

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class ForestChangeDiagnosticTile(
                                       loss: TreeCoverLoss#ITile,
                                       tcd2000: TreeCoverDensity2000#ITile,
                                       isPrimaryForest: PrimaryForest#OptionalITile,
                                       isPeatlands: Peatlands#OptionalITile,
                                       isIntactForestLandscapes2016: IntactForestLandscapes2016#OptionalITile,
                                       wdpaProtectedAreas: ProtectedAreas#OptionalITile,
                                       seAsiaLandCover: SEAsiaLandCover#OptionalITile,
                                       idnLandCover: IndonesiaLandCover#OptionalITile,
                                       isSoyPlantedArea: SoyPlantedAreas#OptionalITile

) extends CellGrid[Int] {

  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
