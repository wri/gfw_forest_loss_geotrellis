package org.globalforestwatch.gladalerts

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class GladAlertsTile(glad: GladAlerts#ITile,
                          biomass: BiomassPerHectar#DTile,
                          climateMask: ClimateMask#OptionalITile)
  extends CellGrid[Int] {
  def cellType: CellType = glad.cellType

  def cols: Int = glad.cols

  def rows: Int = glad.rows
}
