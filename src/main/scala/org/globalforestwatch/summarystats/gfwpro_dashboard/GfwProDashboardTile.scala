package org.globalforestwatch.summarystats.gfwpro_dashboard

import geotrellis.raster.{CellGrid, CellType, IntCellType}
import org.globalforestwatch.layers._

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class GfwProDashboardTile(
  integratedAlerts: IntegratedAlerts#OptionalITile,
  tcd2000: TreeCoverDensityPercent2000#ITile,
  sbtnNaturalForest: SBTNNaturalForests#OptionalITile,
  jrcForestCover: JRCForestCover#OptionalITile,
  gadm0: GadmAdm0#OptionalITile,
  gadm1: GadmAdm1#OptionalITile,
  gadm2: GadmAdm2#OptionalITile
) extends CellGrid[Int] {

  def cellType: CellType = integratedAlerts.cellType.getOrElse(IntCellType)

  def cols: Int = integratedAlerts.cols.getOrElse(GfwProDashboardGrid.blockSize)

  def rows: Int = integratedAlerts.rows.getOrElse(GfwProDashboardGrid.blockSize)
}
