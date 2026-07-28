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
    lazy val grossCumulAbovegroundRemovalsCo2 = sources.grossCumulAbovegroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
    lazy val grossCumulBelowgroundRemovalsCo2 = sources.grossCumulBelowgroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
    lazy val netFluxCo2 = sources.netFluxCo2e.fetchWindow(windowKey, windowLayout)
    lazy val grossEmissionsCo2eCo2OnlyBiomassSoil = sources.grossEmissionsCo2eCo2OnlyBiomassSoil.fetchWindow(windowKey, windowLayout)
    lazy val grossEmissionsCo2eCh4BiomassSoil = sources.grossEmissionsCo2eCh4BiomassSoil.fetchWindow(windowKey, windowLayout)
    lazy val grossEmissionsCo2eN2oBiomassSoil = sources.grossEmissionsCo2eN2oBiomassSoil.fetchWindow(windowKey, windowLayout)
    lazy val grossEmissionsCo2eCo2OnlyBiomassOnly = sources.grossEmissionsCo2eCo2OnlyBiomassOnly.fetchWindow(windowKey, windowLayout)
    lazy val grossEmissionsCo2eCh4BiomassOnly = sources.grossEmissionsCo2eCh4BiomassOnly.fetchWindow(windowKey, windowLayout)
    lazy val grossEmissionsCo2eN2oBiomassOnly = sources.grossEmissionsCo2eN2oBiomassOnly.fetchWindow(windowKey, windowLayout)
    lazy val fluxModelExtent = sources.fluxModelExtent.fetchWindow(windowKey, windowLayout)

    lazy val primaryForest = sources.primaryForest.fetchWindow(windowKey, windowLayout)
    lazy val plantedForestsBool = sources.plantedForestsBool.fetchWindow(windowKey, windowLayout)
    lazy val globalPeat = sources.globalPeat.fetchWindow(windowKey, windowLayout)
    lazy val tclDriverClass = sources.tclDriver.fetchWindow(windowKey, windowLayout)
    lazy val treeCoverLossFromFires = sources.treeCoverLossFromFires.fetchWindow(windowKey, windowLayout)
    lazy val ifl2000 = sources.ifl2000.fetchWindow(windowKey, windowLayout)

    lazy val gpwGrasslandExtent2020 = sources.gpwGrasslandExtent2020.fetchWindow(windowKey, windowLayout)
    lazy val gpwGrasslandExtent2021 = sources.gpwGrasslandExtent2021.fetchWindow(windowKey, windowLayout)
    lazy val gpwGrasslandExtent2022 = sources.gpwGrasslandExtent2022.fetchWindow(windowKey, windowLayout)
    lazy val gpwGrasslandExtent2023 = sources.gpwGrasslandExtent2023.fetchWindow(windowKey, windowLayout)
    lazy val gpwGrasslandExtent2024 = sources.gpwGrasslandExtent2024.fetchWindow(windowKey, windowLayout)

    lazy val plantationsPre2000 = sources.plantationsPre2000.fetchWindow(windowKey, windowLayout)
    lazy val mangroveBiomassExtent = sources.mangroveBiomassExtent.fetchWindow(windowKey, windowLayout)

    def cellType: CellType = loss.cellType

    def cols: Int = loss.cols

    def rows: Int = loss.rows
}
