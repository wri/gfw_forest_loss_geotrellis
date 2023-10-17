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
                           biomass: BiomassPerHectar#OptionalDTile,
                           climateMask: ClimateMask#OptionalITile,
                           primaryForest: PrimaryForest#OptionalITile,
                           protectedAreas: ProtectedAreas#OptionalITile,
                           aze: Aze#OptionalITile,
                           keyBiodiversityAreas: KeyBiodiversityAreas#OptionalITile,
                           landmark: Landmark#OptionalITile,
                           plantedForests: PlantedForests#OptionalITile,
                           mining: Mining#OptionalITile,
                           logging: Logging#OptionalITile,
                           rspo: RSPO#OptionalITile,
                           woodFiber: WoodFiber#OptionalITile,
                           peatlands: Peatlands#OptionalITile,
                           indonesiaForestMoratorium: IndonesiaForestMoratorium#OptionalITile,
                           oilPalm: OilPalm#OptionalITile,
                           indonesiaForestArea: IndonesiaForestArea#OptionalITile,
                           peruForestConcessions: PeruForestConcessions#OptionalITile,
                           oilGas: OilGas#OptionalITile,
                           mangroves2016: Mangroves2016#OptionalITile,
                           intactForestLandscapes2016: IntactForestLandscapes2016#OptionalITile,
                           brazilBiomes: BrazilBiomes#OptionalITile
                         ) extends CellGrid[Int] {

  def cellType: CellType = gladL.cellType.getOrElse(IntCellType)

  def cols: Int = gladL.cols.getOrElse(IntegratedAlertsGrid.blockSize)

  def rows: Int = gladL.rows.getOrElse(IntegratedAlertsGrid.blockSize)
}
