package org.globalforestwatch.summarystats.gfwpro_dashboard

import geotrellis.raster.{CellGrid, CellType, IntCellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class GfwProDashboardTile(
  gladAlerts: IntegratedAlerts#OptionalITile,
  tcd2000: TreeCoverDensityPercent2000#ITile
) extends CellGrid[Int] {

  def cellType: CellType = gladAlerts.cellType.getOrElse(IntCellType)

  def cols: Int = gladAlerts.cols.getOrElse(GfwProDashboardGrid.blockSize)

  def rows: Int = gladAlerts.rows.getOrElse(GfwProDashboardGrid.blockSize)
}
