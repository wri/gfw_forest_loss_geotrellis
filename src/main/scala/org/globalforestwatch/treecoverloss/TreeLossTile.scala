package org.globalforestwatch.treecoverloss

import geotrellis.raster.{CellGrid, CellType, Tile}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  *
  * @param loss
  * @param gain
  * @param tcd2000
  * @param tcd2010
  * @param co2Pixel
  * @param biomass
  * @param mangroveBiomass
  * @param drivers
  * @param globalLandCover
  * @param primaryForest
  * @param idnPrimaryForest
  * @param erosion
  * @param biodiversitySignificance
  * @param wdpa
  * @param plantations
  * @param riverBasins
  * @param ecozones
  * @param urbanWatersheds
  * @param mangroves1996
  * @param mangroves2016
  * @param waterStress
  * @param intactForestLandscapes
  * @param endemicBirdAreas
  * @param tigerLandscapes
  * @param landmark
  * @param landRights
  * @param keyBiodiversityAreas
  * @param mining
  * @param rspo
  * @param peatlands
  * @param oilPalm
  * @param idnForestMoratorium
  * @param idnLandCover
  * @param mexProtectedAreas
  * @param mexPaymentForEcosystemServices
  * @param mexForestZoning
  * @param perProductionForest
  * @param perProtectedAreas
  * @param perForestConcessions
  * @param braBiomes
  * @param woodFiber
  * @param resourceRights
  * @param logging
  */
case class TreeLossTile(
                         loss: TreeCoverLoss#ITile,
                         gain: TreeCoverGain#ITile,
                         tcd2000: TreeCoverDensity#ITile,
                         tcd2010: TreeCoverDensity#ITile,
                         co2Pixel: Carbon#DTile,
                         biomass: BiomassPerHectar#DTile,
                         mangroveBiomass: MangroveBiomass#OptionalDTile,
                         drivers: TreeCoverLossDrivers#OptionalITile,
                         globalLandCover: GlobalLandcover#OptionalITile,
                         primaryForest: PrimaryForest#OptionalITile,
                         idnPrimaryForest: IndonesiaPrimaryForest#OptionalITile,
                         erosion: Erosion#OptionalITile,
                         biodiversitySignificance: BiodiversitySignificance#OptionalITile,
                         wdpa: ProtectedAreas#OptionalITile,
                         plantations: Plantations#OptionalITile,
                         riverBasins: RiverBasins#OptionalITile,
                         ecozones: Ecozones#OptionalITile,
                         urbanWatersheds: UrbanWatersheds#OptionalITile,
                         mangroves1996: Mangroves1996#OptionalITile,
                         mangroves2016: Mangroves2016#OptionalITile,
                         waterStress: WaterStress#OptionalITile,
                         intactForestLandscapes: IntactForestLandscapes#OptionalITile,
                         endemicBirdAreas: EndemicBirdAreas#OptionalITile,
                         tigerLandscapes: TigerLandscapes#OptionalITile,
                         landmark: Landmark#OptionalITile,
                         landRights: LandRights#OptionalITile,
                         keyBiodiversityAreas: KeyBiodiversityAreas#OptionalITile,
                         mining: Mining#OptionalITile,
                         rspo: RSPO#OptionalITile,
                         peatlands: Peatlands#OptionalITile,
                         oilPalm: OilPalm#OptionalITile,
                         idnForestMoratorium: IndonesiaForestMoratorium#OptionalITile,
                         idnLandCover: IndonesiaLandCover#OptionalITile,
                         mexProtectedAreas: MexicoProtectedAreas#OptionalITile,
                         mexPaymentForEcosystemServices: MexicoPaymentForEcosystemServices#OptionalITile,
                         mexForestZoning: MexicoForestZoning#OptionalITile,
                         perProductionForest: PeruProductionForest#OptionalITile,
                         perProtectedAreas: PeruProtectedAreas#OptionalITile,
                         perForestConcessions: PeruForestConcessions#OptionalITile,
                         braBiomes: BrazilBiomes#OptionalITile,
                         woodFiber: WoodFiber#OptionalITile,
                         resourceRights: ResourceRights#OptionalITile,
                         logging: Logging#OptionalITile
  
) extends CellGrid {
  def cellType: CellType = loss.cellType
  def cols: Int = loss.cols
  def rows: Int = loss.rows
}