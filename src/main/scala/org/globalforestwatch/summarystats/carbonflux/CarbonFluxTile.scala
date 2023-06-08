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
                           gain: TreeCoverGain#OptionalITile,
                           tcd2000: TreeCoverDensityThreshold#ITile,
                           biomass: BiomassPerHectar#OptionalDTile,
                           grossAnnualAbovegroundRemovalsCarbon: GrossAnnualAbovegroundRemovalsCarbon#OptionalFTile,
                           grossAnnualBelowgroundRemovalsCarbon: GrossAnnualBelowgroundRemovalsCarbon#OptionalFTile,
                           grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2#OptionalFTile,
                           grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2#OptionalFTile,
                           netFluxCo2: NetFluxCo2e#OptionalFTile,
                           agcEmisYear: AgcEmisYear#OptionalFTile,
                           bgcEmisYear: BgcEmisYear#OptionalFTile,
                           deadwoodCarbonEmisYear: DeadwoodCarbonEmisYear#OptionalFTile,
                           litterCarbonEmisYear: LitterCarbonEmisYear#OptionalFTile,
                           soilCarbonEmisYear: SoilCarbonEmisYear#OptionalFTile,
                           abovegroundCarbon2000: AbovegroundCarbon2000#OptionalFTile,
                           belowgroundCarbon2000: BelowgroundCarbon2000#OptionalFTile,
                           deadwoodCarbon2000: DeadwoodCarbon2000#OptionalFTile,
                           litterCarbon2000: LitterCarbon2000#OptionalFTile,
                           soilCarbon2000: SoilCarbon2000#OptionalFTile,
                           grossEmissionsCo2eNonCo2BiomassSoil: GrossEmissionsNonCo2Co2e#OptionalFTile,
                           grossEmissionsCo2eCo2OnlyBiomassSoil: GrossEmissionsCo2OnlyCo2e#OptionalFTile,
                           grossEmissionsCo2eNonCo2SoilOnly: GrossEmissionsNonCo2Co2eSoilOnly#OptionalFTile,
                           grossEmissionsCo2eCo2OnlySoilOnly: GrossEmissionsCo2OnlyCo2eSoilOnly#OptionalFTile,
                           jplTropicsAbovegroundBiomassDensity2000: JplTropicsAbovegroundBiomassDensity2000#OptionalFTile,
                           stdevAnnualAbovegroundRemovalsCarbon: StdevAnnualAbovegroundRemovalsCarbon#OptionalFTile,
                           stdevSoilCarbon2000: StdevSoilCarbon2000#OptionalFTile,

                           fluxModelExtent: FluxModelExtent#OptionalITile,
                           mangroveBiomassExtent: MangroveBiomassExtent#OptionalITile,
                           removalForestType: RemovalForestType#OptionalITile,
                           drivers: TreeCoverLossDrivers#OptionalITile,
                           faoEcozones2000: FaoEcozones2000#OptionalITile,
                           landmark: Landmark#OptionalITile,
                           wdpa: ProtectedAreas#OptionalITile,
                           intactForestLandscapes2000: IntactForestLandscapes2000#OptionalITile,
                           plantationsTypeFluxModel: PlantationsTypeFluxModel#OptionalITile,
                           intactPrimaryForest: IntactPrimaryForest#OptionalITile,
                           peatlands: Peatlands#OptionalITile,
                           forestAgeCategory: ForestAgeCategory#OptionalITile,
                           jplTropicsAbovegroundBiomassExtent2000: JplTropicsAbovegroundBiomassExtent2000#OptionalITile,
                           fiaRegionsUsExtent: FiaRegionsUsExtent#OptionalITile,
                           braBiomes: BrazilBiomes#OptionalITile,
                           riverBasins: RiverBasins#OptionalITile,
                           primaryForest: PrimaryForest#OptionalITile,
                           lossLegalAmazon: TreeCoverLossLegalAmazon#OptionalITile,
                           prodesLegalAmazonExtent2000: ProdesLegalAmazonExtent2000#OptionalITile,
                           tropicLatitudeExtent: TropicLatitudeExtent#OptionalITile,
                           treeCoverLossFromFires: TreeCoverLossFromFires#OptionalITile,
                           grossEmissionsNodeCodes: GrossEmissionsNodeCodes#OptionalITile
                         ) extends CellGrid[Int] {
  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
