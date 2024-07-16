package org.globalforestwatch.summarystats.gfwpro_dashboard

import geotrellis.raster.{CellGrid, CellType, IntCellType}
import geotrellis.layer.{LayoutDefinition, SpatialKey}

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class GfwProDashboardTile(
  windowKey: SpatialKey,
  windowLayout: LayoutDefinition,
  sources: GfwProDashboardGridSources,
) extends CellGrid[Int] {

  lazy val integratedAlerts = sources.integratedAlerts.fetchWindow(windowKey, windowLayout)
  lazy val tcd2000 = sources.treeCoverDensity2000.fetchWindow(windowKey, windowLayout)
  lazy val sbtnNaturalForest = sources.sbtnNaturalForest.fetchWindow(windowKey, windowLayout)
  lazy val jrcForestCover = sources.jrcForestCover.fetchWindow(windowKey, windowLayout)
  lazy val gadm0 = sources.gadmAdm0.fetchWindow(windowKey, windowLayout)
  lazy val gadm1 = sources.gadmAdm1.fetchWindow(windowKey, windowLayout)
  lazy val gadm2 = sources.gadmAdm2.fetchWindow(windowKey, windowLayout)

  def cellType: CellType = integratedAlerts.cellType.getOrElse(IntCellType)

  def cols: Int = integratedAlerts.cols.getOrElse(GfwProDashboardGrid.blockSize)

  def rows: Int = integratedAlerts.rows.getOrElse(GfwProDashboardGrid.blockSize)
}
