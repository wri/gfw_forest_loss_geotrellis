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
                                  gain: TreeCoverGain#ITile,
                                  tcd2000: TreeCoverDensity#ITile,
                                  biomass: BiomassPerHectar#OptionalDTile,
                                  //                           grossAnnualRemovalsCarbon: GrossAnnualRemovalsBiomass#OptionalDTile,
                                  grossCumulRemovalsCarbon: GrossCumulRemovalsCo2#OptionalDTile,
                                  netFluxCo2: NetFluxCo2e#OptionalDTile,
                                  agcEmisYear: AgcEmisYear#OptionalFTile,
                                  //                           bgcEmisYear: BgcEmisYear#OptionalDTile,
                                  //                           deadwoodCarbonEmisYear: DeadwoodCarbonEmisYear#OptionalDTile,
                                  //                           litterCarbonEmisYear: LitterCarbonEmisYear#OptionalDTile,
                                  soilCarbonEmisYear: SoilCarbonEmisYear#OptionalDTile,
                                  //                           totalCarbonEmisYear: TotalCarbonEmisYear#OptionalDTile,
                                  agc2000: Agc2000#OptionalFTile,
                                  //                           bgc2000: Bgc2000#OptionalDTile,
                                  //                           deadwoodCarbon2000: DeadwoodCarbon2000#OptionalDTile,
                                  //                           litterCarbon2000: LitterCarbon2000#OptionalDTile,
                                  soilCarbon2000: SoilCarbon2000#OptionalDTile,
                                  //                           totalCarbon2000: TotalCarbon2000#OptionalDTile,
                                  grossEmissionsCo2eNoneCo2: GrossEmissionsNonCo2Co2e#OptionalDTile,
                                  grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2e#OptionalDTile,
                                  mangroveBiomassExtent: MangroveBiomassExtent#OptionalDTile,
                                  drivers: TreeCoverLossDrivers#OptionalITile,
                                  ecozones: Ecozones#OptionalITile,
                                  landRights: LandRights#OptionalITile,
                                  wdpa: ProtectedAreas#OptionalITile,
                                  intactForestLandscapes: IntactForestLandscapes#OptionalITile,
                                  plantations: Plantations#OptionalITile,
                                  intactPrimaryForest: IntactPrimaryForest#OptionalITile,
                                  peatlandsFlux: PeatlandsFlux#OptionalITile,
                                  forestAgeCategory: ForestAgeCategory#OptionalITile
) extends CellGrid {
  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
