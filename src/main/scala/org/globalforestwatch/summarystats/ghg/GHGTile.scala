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
  lazy val grossEmissionsCo2eCo2Only = sources.grossEmissionsCo2eCo2Only.fetchWindow(windowKey, windowLayout)
  lazy val grossEmissionsCo2eCH4 = sources.grossEmissionsCo2eCH4.fetchWindow(windowKey, windowLayout)
  lazy val grossEmissionsCo2eN2O = sources.grossEmissionsCo2eN2O.fetchWindow(windowKey, windowLayout)
  // It is important that the fetches for the individual yield files and the gadm
  // admin areas are lazy, since we will only need access to at most one commodity
  // file for any particular location. And we only need to access the gadm area
  // values if we can't get a yield value from the relevant commodity file.
  lazy val cocoYield = sources.mapspamCOCOYield.fetchWindow(windowKey, windowLayout)
  lazy val coffYield = sources.mapspamCOFFYield.fetchWindow(windowKey, windowLayout)
  lazy val oilpYield = sources.mapspamOILPYield.fetchWindow(windowKey, windowLayout)
  lazy val rubbYield = sources.mapspamRUBBYield.fetchWindow(windowKey, windowLayout)
  lazy val soybYield = sources.mapspamSOYBYield.fetchWindow(windowKey, windowLayout)
  lazy val sugcYield = sources.mapspamSUGCYield.fetchWindow(windowKey, windowLayout)
  lazy val gadmAdm0 = sources.gadmAdm0.fetchWindow(windowKey, windowLayout)
  lazy val gadmAdm1 = sources.gadmAdm1.fetchWindow(windowKey, windowLayout)
  lazy val gadmAdm2 = sources.gadmAdm2.fetchWindow(windowKey, windowLayout)
  lazy val treeCoverGainFromHeight = sources.treeCoverGainFromHeight.fetchWindow(windowKey, windowLayout)
  lazy val mangroveBiomassExtent = sources.mangroveBiomassExtent.fetchWindow(windowKey, windowLayout)

  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
