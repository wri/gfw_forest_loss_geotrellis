package org.globalforestwatch.summarystats.gfwpro_dashboard

import geotrellis.raster.{CellGrid, CellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class GfwProDashboardTile(
                                gladAlerts: GladAlerts#OptionalITile,


                              ) extends CellGrid[Int] {

  def cellType: CellType = gladAlerts.cellType

  def cols: Int = gladAlerts.cols

  def rows: Int = gladAlerts.rows
}
