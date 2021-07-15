package org.globalforestwatch.summarystats.firealerts

import geotrellis.raster.{CellGrid, CellType, IntCellType}
import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.layers.{TreeCoverDensityThreshold2000, _}

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class FireAlertsTile(
                           gridTile: GridTile,
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
                           intactForestLandscapes2016: IntactForestLandscapes2016#OptionalITile,
                           brazilBiomes: BrazilBiomes#OptionalITile,
                           tcd2000: TreeCoverDensityThreshold2000#ITile
                         ) extends CellGrid[Int] {

  def cellType: CellType = IntCellType

  def cols: Int = gridTile.blockSize

  def rows: Int = gridTile.blockSize
}
