package org.globalforestwatch.summarystats.annualupdate_minimal

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class AnnualUpdateMinimalTile(
                                    loss: TreeCoverLoss#ITile,
                                    gain: TreeCoverGain#OptionalITile,
                                    tcd2000: TreeCoverDensityThreshold#ITile,
                                    tcd2010: TreeCoverDensityThreshold#ITile,
                                    biomass: AbovegroundBiomass2000#OptionalDTile,
                                    drivers: TreeCoverLossDrivers#OptionalITile,
                                    primaryForest: PrimaryForest#OptionalITile,
                                    wdpa: ProtectedAreas#OptionalITile,
                                    aze: Aze#OptionalITile,
                                    plantedForests: PlantedForests#OptionalITile,
                                    mangroves1996: Mangroves1996#OptionalITile,
                                    mangroves2020: Mangroves2020#OptionalITile,
                                    tigerLandscapes: TigerLandscapes#OptionalITile,
                                    landmark: Landmark#OptionalITile,
                                    keyBiodiversityAreas: KeyBiodiversityAreas#OptionalITile,
                                    mining: Mining#OptionalITile,
                                    peatlands: Peatlands#OptionalITile,
                                    oilPalm: OilPalm#OptionalITile,
                                    idnForestMoratorium: IndonesiaForestMoratorium#OptionalITile,
                                    woodFiber: WoodFiber#OptionalITile,
                                    resourceRights: ResourceRights#OptionalITile,
                                    logging: Logging#OptionalITile,
                                    grossEmissionsCo2eNonCo2: GrossEmissionsNonCo2Co2eBiomassSoil#OptionalFTile,
                                    grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2BiomassSoil#OptionalFTile,
                                    grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2#OptionalFTile,
                                    grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2#OptionalFTile,
                                    netFluxCo2: NetFluxCo2e#OptionalFTile,
                                    soilCarbon2000: SoilCarbon2000#OptionalFTile,
                                    intactForestLandscapes2000: IntactForestLandscapes2000#OptionalITile,
                                    treeCoverLossFromFires: TreeCoverLossFromFires#OptionalITile,
                                    tropicalTreeCover: TropicalTreeCover#OptionalITile,
                                    umdGlobalLandCover: UmdGlobalLandcover#OptionalITile,
                                    plantationsPre2000: PlantationsPre2000#OptionalITile,
                                    abovegroundCarbon2000: AbovegroundCarbon2000#OptionalFTile,
                                    belowgroundCarbon2000: BelowgroundCarbon2000#OptionalFTile,
                                    mangroveBiomassExtent: MangroveBiomassExtent#OptionalITile,
                                  ) extends CellGrid[Int] {
  def cellType: CellType = loss.cellType
  def cols: Int = loss.cols
  def rows: Int = loss.rows
}
