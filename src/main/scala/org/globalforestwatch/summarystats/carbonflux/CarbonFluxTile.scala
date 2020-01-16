package org.globalforestwatch.summarystats.carbonflux

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class CarbonFluxTile(
                           loss: TreeCoverLoss#ITile,
                           gain: TreeCoverGain#ITile,
                           tcd2000: TreeCoverDensity#ITile,
                           biomass: BiomassPerHectar#OptionalDTile,
                           grossAnnualRemovalsCarbon: GrossAnnualRemovalsBiomass#OptionalFTile,
                           grossCumulRemovalsCarbon: GrossCumulRemovalsCo2#OptionalFTile,
                           netFluxCo2: NetFluxCo2e#OptionalFTile,
                           agcEmisYear: AgcEmisYear#OptionalFTile,
                           bgcEmisYear: BgcEmisYear#OptionalFTile,
                           deadwoodCarbonEmisYear: DeadwoodCarbonEmisYear#OptionalFTile,
                           litterCarbonEmisYear: LitterCarbonEmisYear#OptionalFTile,
                           soilCarbonEmisYear: SoilCarbonEmisYear#OptionalFTile,
                           totalCarbonEmisYear: TotalCarbonEmisYear#OptionalFTile,
                           agc2000: Agc2000#OptionalFTile,
                           bgc2000: Bgc2000#OptionalFTile,
                           deadwoodCarbon2000: DeadwoodCarbon2000#OptionalFTile,
                           litterCarbon2000: LitterCarbon2000#OptionalFTile,
                           soilCarbon2000: SoilCarbon2000#OptionalFTile,
                           totalCarbon2000: TotalCarbon2000#OptionalFTile,
                           grossEmissionsCo2eNoneCo2: GrossEmissionsNonCo2Co2e#OptionalFTile,
                           grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2e#OptionalFTile,
                           jplTropicsAbovegroundBiomassDensity2000: JplTropicsAbovegroundBiomassDensity2000#OptionalFTile,
                           mangroveBiomassExtent: MangroveBiomassExtent#OptionalDTile,
                           drivers: TreeCoverLossDrivers#OptionalITile,
                           ecozones: Ecozones#OptionalITile,
                           landRights: LandRights#OptionalITile,
                           wdpa: ProtectedAreas#OptionalITile,
                           intactForestLandscapes: IntactForestLandscapes#OptionalITile,
                           plantations: Plantations#OptionalITile,
                           intactPrimaryForest: IntactPrimaryForest#OptionalITile,
                           peatlandsFlux: PeatlandsFlux#OptionalITile,
                           forestAgeCategory: ForestAgeCategory#OptionalITile,
                           jplTropicsAbovegroundBiomassExtent2000: JplTropicsAbovegroundBiomassExtent2000#OptionalITile,
                           fiaRegionsUsExtent: FiaRegionsUsExtent#OptionalITile,
                           braBiomes: BrazilBiomes#OptionalITile,
                           riverBasins: RiverBasins#OptionalITile,
                           primaryForest: PrimaryForest#OptionalITile,
                           lossLegalAmazon: TreeCoverLossLegalAmazon#OptionalITile,
                           prodesLegalAmazonExtent2000: ProdesLegalAmazonExtent2000#OptionalITile
                         ) extends CellGrid {
  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
