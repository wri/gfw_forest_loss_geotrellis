package org.globalforestwatch.summarystats.carbonflux_minimal

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class CarbonFluxMinimalTile(loss: TreeCoverLoss#ITile,
                                 gain: TreeCoverGain#ITile,
                                 tcd2000: TreeCoverDensity2000#ITile,   // Same as treecoverloss package
                                 tcd2010: TreeCoverDensity2010#ITile,
                                 biomass: BiomassPerHectar#OptionalDTile,
                                 primaryForest: PrimaryForest#OptionalITile,
                                 plantationsBool: PlantationsBool#OptionalITile,

                                 grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2#OptionalFTile,
                                 grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2#OptionalFTile,
                                 netFluxCo2: NetFluxCo2e#OptionalFTile,
                                 grossEmissionsCo2eNonCo2: GrossEmissionsNonCo2Co2e#OptionalFTile,
                                 grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2e#OptionalFTile,
                                 fluxModelExtent: FluxModelExtent#OptionalITile)

  extends CellGrid[Int] {

  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
