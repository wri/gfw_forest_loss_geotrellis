package org.globalforestwatch.gladalerts

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridId, TenByTenGrid}

object GladAlertsGrid extends TenByTenGrid[GladAlertsGridSources] {

  val gridExtent = Extent(-180.0000, -30.0000, 180.0000, 30.0000)

  def getSources(gridId: String): GladAlertsGridSources = GladAlertsGridSources.getCachedSources(gridId)

  def checkSources(gridId: String, windowExtent: Extent): GladAlertsGridSources = {
    val sources = getSources(gridId)
    checkRequired(sources.gladAlerts, windowExtent)
    checkOptional(sources.biomassPerHectar, windowExtent)
    checkOptional(sources.climateMask, windowExtent)
    checkOptional(sources.primaryForest, windowExtent)
    checkOptional(sources.protectedAreas, windowExtent)
    checkOptional(sources.aze, windowExtent)
    checkOptional(sources.keyBiodiversityAreas, windowExtent)
    checkOptional(sources.landmark, windowExtent)
    checkOptional(sources.plantations, windowExtent)
    checkOptional(sources.mining, windowExtent)
    checkOptional(sources.logging, windowExtent)
    checkOptional(sources.rspo, windowExtent)
    checkOptional(sources.woodFiber, windowExtent)
    checkOptional(sources.peatlands, windowExtent)
    checkOptional(sources.indonesiaForestMoratorium, windowExtent)
    checkOptional(sources.oilPalm, windowExtent)
    checkOptional(sources.indonesiaForestArea, windowExtent)
    checkOptional(sources.peruForestConcessions, windowExtent)
    checkOptional(sources.oilPalm, windowExtent)
    checkOptional(sources.mangroves2016, windowExtent)
    checkOptional(sources.intactForestLandscapes2016, windowExtent)
    checkOptional(sources.braBiomes, windowExtent)

    sources
  }


}
