package org.globalforestwatch.gladalerts

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class GladAlertsTile(
                           glad: GladAlerts#ITile,
                           biomass: BiomassPerHectar#DTile,
                           climateMask: ClimateMask#OptionalITile,
                           primaryForest: PrimaryForest#OptionalITile,
                           protectedAreas: ProtectedAreas#OptionalITile,
                           aze: Aze#OptionalITile,
                           keyBiodiversityAreas: KeyBiodiversityAreas#OptionalITile,
                           landmark: Landmark#OptionalITile,
                           plantations: Plantations#OptionalITile,
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
                           intactForestLandscapes2016: IntactForestLandscapes2016#OptionalITile
                         ) extends CellGrid[Int] {
  def cellType: CellType = glad.cellType

  def cols: Int = glad.cols

  def rows: Int = glad.rows
}
