package org.globalforestwatch.summarystats.treecoverloss

import geotrellis.raster.{CellGrid, CellType}
import geotrellis.layer.{LayoutDefinition, SpatialKey}

/**
  *
  * Tile-like structure to hold tiles from datasets required for our summary.
  * We can not use GeoTrellis MultibandTile because it requires all bands share a CellType.
  */
case class TreeLossTile(
                         windowKey: SpatialKey,
                         windowLayout: LayoutDefinition,
                         sources: TreeLossGridSources,
                       )
  extends CellGrid[Int] {

    lazy val loss = sources.treeCoverLoss.fetchWindow(windowKey, windowLayout)
    lazy val gain = sources.treeCoverGain.fetchWindow(windowKey, windowLayout)
    lazy val tcd2000 = sources.treeCoverDensity2000.fetchWindow(windowKey, windowLayout)
    lazy val tcd2010 = sources.treeCoverDensity2010.fetchWindow(windowKey, windowLayout)
    lazy val biomass = sources.biomassPerHectar.fetchWindow(windowKey, windowLayout)
    lazy val agc2000 = sources.agc2000.fetchWindow(windowKey, windowLayout)
    lazy val bgc2000 = sources.bgc2000.fetchWindow(windowKey, windowLayout)
    lazy val soilCarbon2000 = sources.soilCarbon2000.fetchWindow(windowKey, windowLayout)
    lazy val primaryForest = sources.primaryForest.fetchWindow(windowKey, windowLayout)
    lazy val plantedForestsBool = sources.plantedForestsBool.fetchWindow(windowKey, windowLayout)
    lazy val plantationsPre2000 = sources.plantationsPre2000.fetchWindow(windowKey, windowLayout)
    lazy val mangroveBiomassExtent = sources.mangroveBiomassExtent.fetchWindow(windowKey, windowLayout)
    lazy val grossCumulAbovegroundRemovalsCo2 = sources.grossCumulAbovegroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
    lazy val grossCumulBelowgroundRemovalsCo2 = sources.grossCumulBelowgroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
    lazy val netFluxCo2 = sources.netFluxCo2e.fetchWindow(windowKey, windowLayout)
    lazy val grossEmissionsCo2eNonCo2 = sources.grossEmissionsCo2eNonCo2.fetchWindow(windowKey, windowLayout)
    lazy val grossEmissionsCo2eCo2Only = sources.grossEmissionsCo2eCo2Only.fetchWindow(windowKey, windowLayout)
    lazy val fluxModelExtent = sources.fluxModelExtent.fetchWindow(windowKey, windowLayout)

    def cellType: CellType = loss.cellType

    def cols: Int = loss.cols

    def rows: Int = loss.rows
}
