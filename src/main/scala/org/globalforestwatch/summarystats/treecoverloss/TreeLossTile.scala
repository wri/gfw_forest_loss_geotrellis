package org.globalforestwatch.summarystats.treecoverloss

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class TreeLossTile(loss: TreeCoverLoss#ITile,
                        gain: TreeCoverGain#ITile,
                        tcd2000: TreeCoverDensityPercent2000#ITile,
                        tcd2010: TreeCoverDensityPercent2010#ITile,
                        biomass: BiomassPerHectar#OptionalDTile,
                        agc2000: Agc2000#OptionalFTile,
                        bgc2000: Bgc2000#OptionalFTile,
                        soilCarbon2000: SoilCarbon2000#OptionalFTile,
                        primaryForest: PrimaryForest#OptionalITile,
                        plantationsBool: PlantationsBool#OptionalITile,
                        grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2#OptionalFTile,
                        grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2#OptionalFTile,
                        netFluxCo2: NetFluxCo2e#OptionalFTile,
                        grossEmissionsCo2eNonCo2: GrossEmissionsNonCo2Co2eBiomassSoil#OptionalFTile,
                        grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2eBiomassSoil#OptionalFTile,
                        fluxModelExtent: FluxModelExtent#OptionalITile)

  extends CellGrid[Int] {

  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
