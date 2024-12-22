package org.globalforestwatch.summarystats.ghg

import geotrellis.raster.{CellGrid, CellType}
import geotrellis.layer.{LayoutDefinition, SpatialKey}

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class GHGTile(
  windowKey: SpatialKey,
  windowLayout: LayoutDefinition,
  sources: GHGGridSources,
) extends CellGrid[Int] {

  lazy val loss = sources.treeCoverLoss.fetchWindow(windowKey, windowLayout)
  lazy val tcd2000 = sources.treeCoverDensity2000.fetchWindow(windowKey, windowLayout)
  lazy val grossEmissionsCo2eNonCo2 = sources.grossEmissionsCo2eNonCo2.fetchWindow(windowKey, windowLayout)
  lazy val grossEmissionsCo2eCo2Only = sources.grossEmissionsCo2eCo2Only.fetchWindow(windowKey, windowLayout)
  lazy val cocoYield = sources.mapspamCOCOYield.fetchWindow(windowKey, windowLayout)
  lazy val coffYield = sources.mapspamCOFFYield.fetchWindow(windowKey, windowLayout)
  lazy val oilpYield = sources.mapspamOILPYield.fetchWindow(windowKey, windowLayout)
  lazy val rubbYield = sources.mapspamRUBBYield.fetchWindow(windowKey, windowLayout)
  lazy val soybYield = sources.mapspamSOYBYield.fetchWindow(windowKey, windowLayout)

  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
