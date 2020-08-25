package org.globalforestwatch.summarystats.carbonflux_custom_area

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class CarbonCustomTile(
                             loss: TreeCoverLoss#ITile,
                             gain: TreeCoverGain#ITile,
                             tcd2000: TreeCoverDensity#ITile,
                             biomass: BiomassPerHectar#OptionalDTile,
                             grossAnnualRemovalsCarbon: GrossAnnualAbovegroundRemovalsCarbon#OptionalFTile,
                             grossCumulRemovalsCarbon: GrossCumulAbovegroundRemovalsCo2#OptionalFTile,
                             netFluxCo2: NetFluxCo2e#OptionalFTile,
                             grossEmissionsCo2eNoneCo2: GrossEmissionsNonCo2Co2e#OptionalFTile,
                             grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2e#OptionalFTile,
                             carbonFluxCustomArea1: CarbonFluxCustomArea1#OptionalITile
                         ) extends CellGrid {
  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
