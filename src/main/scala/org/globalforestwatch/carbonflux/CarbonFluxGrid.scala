package org.globalforestwatch.carbonflux

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{TenByTenGrid, GridId}

object CarbonFluxGrid extends TenByTenGrid[CarbonFluxGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String) =
    CarbonFluxGridSources.getCachedSources(gridId)

  def checkSources(gridId: String,
                   windowExtent: Extent): CarbonFluxGridSources = {

    val sources: CarbonFluxGridSources = getSources(gridId)

    checkRequired(sources.treeCoverLoss, windowExtent)
    checkRequired(sources.treeCoverGain, windowExtent)
    checkRequired(sources.treeCoverDensity2000, windowExtent)

    checkOptional(sources.biomassPerHectar, windowExtent)
    checkOptional(sources.grossAnnualRemovalsCarbon, windowExtent)
    checkOptional(sources.grossCumulRemovalsCarbon, windowExtent)
    checkOptional(sources.netFluxCo2, windowExtent)
    checkOptional(sources.agcEmisYear, windowExtent)
    checkOptional(sources.bgcEmisYear, windowExtent)
    checkOptional(sources.deadwoodCarbonEmisYear, windowExtent)
    checkOptional(sources.litterCarbonEmisYear, windowExtent)
    checkOptional(sources.soilCarbonEmisYear, windowExtent)
    checkOptional(sources.totalCarbonEmisYear, windowExtent)
    checkOptional(sources.agc2000, windowExtent)
    checkOptional(sources.bgc2000, windowExtent)
    checkOptional(sources.deadwoodCarbon2000, windowExtent)
    checkOptional(sources.litterCarbon2000, windowExtent)
    checkOptional(sources.soilCarbon2000, windowExtent)
    checkOptional(sources.totalCarbon2000, windowExtent)
    checkOptional(sources.grossEmissionsCo2, windowExtent)
    checkOptional(sources.mangroveBiomassExtent, windowExtent)
    checkOptional(sources.treeCoverLossDrivers, windowExtent)
    checkOptional(sources.protectedAreas, windowExtent)
    checkOptional(sources.plantations, windowExtent)
    checkOptional(sources.ecozones, windowExtent)
    checkOptional(sources.intactForestLandscapes, windowExtent)
    checkOptional(sources.landRights, windowExtent)

    sources

  }

}
