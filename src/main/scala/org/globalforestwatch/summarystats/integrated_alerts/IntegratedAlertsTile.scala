package org.globalforestwatch.summarystats.integrated_alerts

import geotrellis.raster.{CellGrid, CellType, IntCellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class IntegratedAlertsTile(
                                 gladL: GladAlerts#OptionalITile,
                                 gladS2: GladAlertsS2#OptionalITile,
                                 radd: RaddAlerts#OptionalITile,
                                 biomass: AbovegroundBiomass2000#OptionalDTile,
                                 primaryForest: PrimaryForest#OptionalITile,
                                 protectedAreas: ProtectedAreas#OptionalITile,
                                 landmark: Landmark#OptionalITile,
                                 peatlands: Peatlands#OptionalITile,
                                 mangroves2020: Mangroves2020#OptionalITile,
                                 intactForestLandscapes2016: IntactForestLandscapes2016#OptionalITile,
                                 naturalForests: SBTNNaturalForests#OptionalITile
                         ) extends CellGrid[Int] {

  def cellType: CellType = gladL.cellType.getOrElse(IntCellType)

  def cols: Int = gladL.cols.getOrElse(IntegratedAlertsGrid.blockSize)

  def rows: Int = gladL.rows.getOrElse(IntegratedAlertsGrid.blockSize)
}
