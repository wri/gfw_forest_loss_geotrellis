package org.globalforestwatch.summarystats.forest_change_diagnostic

import geotrellis.raster.{CellGrid, CellType}
import geotrellis.layer.{LayoutDefinition, SpatialKey}

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class ForestChangeDiagnosticTile(
  windowKey: SpatialKey,
  windowLayout: LayoutDefinition,
  sources: ForestChangeDiagnosticGridSources,
) extends CellGrid[Int] {

  lazy val loss = sources.treeCoverLoss.fetchWindow(windowKey, windowLayout)
  lazy val tcd2000 = sources.treeCoverDensity2000.fetchWindow(windowKey, windowLayout)
  lazy val isPrimaryForest = sources.isPrimaryForest.fetchWindow(windowKey, windowLayout)
  lazy val isPeatlands = sources.isPeatlands.fetchWindow(windowKey, windowLayout)
  lazy val isIntactForestLandscapes2000 =
        sources.isIntactForestLandscapes2000.fetchWindow(windowKey, windowLayout)
  lazy val wdpaProtectedAreas = sources.protectedAreas.fetchWindow(windowKey, windowLayout)
  lazy val seAsiaLandCover = sources.seAsiaLandCover.fetchWindow(windowKey, windowLayout)
  lazy val idnLandCover = sources.idnLandCover.fetchWindow(windowKey, windowLayout)
  lazy val isSoyPlantedArea = sources.isSoyPlantedArea.fetchWindow(windowKey, windowLayout)
  lazy val idnForestArea = sources.idnForestArea.fetchWindow(windowKey, windowLayout)
  lazy val isIDNForestMoratorium = sources.isIDNForestMoratorium.fetchWindow(windowKey, windowLayout)
  lazy val prodesLossYear = sources.prodesLossYear.fetchWindow(windowKey, windowLayout)
  lazy val braBiomes = sources.braBiomes.fetchWindow(windowKey, windowLayout)
  lazy val isPlantation = sources.isPlantation.fetchWindow(windowKey, windowLayout)
  lazy val gfwProCoverage = sources.gfwProCoverage.fetchWindow(windowKey, windowLayout)
  lazy val argOTBN = sources.argOTBN.fetchWindow(windowKey, windowLayout)

  lazy val detailedWdpaProtectedAreas = sources.detailedProtectedAreas.fetchWindow(windowKey, windowLayout)
  lazy val landmark = sources.landmark.fetchWindow(windowKey, windowLayout)

  def cellType: CellType = loss.cellType

  def cols: Int = loss.cols

  def rows: Int = loss.rows
}
