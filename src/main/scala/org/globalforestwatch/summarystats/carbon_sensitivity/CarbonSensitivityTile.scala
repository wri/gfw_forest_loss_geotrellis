package org.globalforestwatch.summarystats.carbon_sensitivity

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
 *
 * Tile-like structure to hold tiles from datasets required for our summary.
 * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
 */
case class CarbonSensitivityTile(
                                  loss: TreeCoverLoss#ITile,
                                  gain: TreeCoverGain#OptionalITile,
                                  tcd2000: TreeCoverDensityThreshold#ITile,
                                  biomass: BiomassPerHectar#OptionalDTile,
                                  grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2#OptionalFTile,
                                  grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2#OptionalFTile,
                                  netFluxCo2: NetFluxCo2e#OptionalFTile,
                                  agcEmisYear: AgcEmisYear#OptionalFTile,
                                  soilCarbonEmisYear: SoilCarbonEmisYear#OptionalFTile,
                                  grossEmissionsCo2eNonCo2: GrossEmissionsNonCo2Co2e#OptionalFTile,
                                  grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2e#OptionalFTile,
                                  jplTropicsAbovegroundBiomassDensity2000: JplTropicsAbovegroundBiomassDensity2000#OptionalFTile,

                                  fluxModelExtent: FluxModelExtent#OptionalITile,
                                  removalForestType: RemovalForestType#OptionalITile,
                                  mangroveBiomassExtent: MangroveBiomassExtent#OptionalITile,
                                  drivers: TreeCoverLossDrivers#OptionalITile,
                                  ecozones: Ecozones#OptionalITile,
                                  landmark: Landmark#OptionalITile,
                                  wdpa: ProtectedAreas#OptionalITile,
                                  intactForestLandscapes: IntactForestLandscapes#OptionalITile,
                                  plantationsTypeFluxModel: PlantationsTypeFluxModel#OptionalITile,
                                  intactPrimaryForest: IntactPrimaryForest#OptionalITile,
                                  peatlandsExtentFluxModel: PeatlandsExtentFluxModel#OptionalITile,
                                  forestAgeCategory: ForestAgeCategory#OptionalITile,
                                  jplTropicsAbovegroundBiomassExtent2000: JplTropicsAbovegroundBiomassExtent2000#OptionalITile,
                                  fiaRegionsUsExtent: FiaRegionsUsExtent#OptionalITile,
                                  braBiomes: BrazilBiomes#OptionalITile,
                                  riverBasins: RiverBasins#OptionalITile,
                                  primaryForest: PrimaryForest#OptionalITile,
                                  lossLegalAmazon: TreeCoverLossLegalAmazon#OptionalITile,
                                  prodesLegalAmazonExtent2000: ProdesLegalAmazonExtent2000#OptionalITile,
                                  tropicLatitudeExtent: TropicLatitudeExtent#OptionalITile,
                                  burnYearHansenLoss: BurnYearHansenLoss#OptionalITile,
                                  grossEmissionsNodeCodes: GrossEmissionsNodeCodes#OptionalITile
                                ) extends CellGrid[Int] {
  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}