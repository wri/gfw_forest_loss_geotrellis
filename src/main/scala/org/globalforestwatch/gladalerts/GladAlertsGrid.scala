package org.globalforestwatch.gladalerts

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridId, TenByTenGrid}

object GladAlertsGrid extends TenByTenGrid {

  val gridExtent = Extent(-180.0000, -30.0000, 180.0000, 30.0000)

  def getSources(gridId: String): GladAlertsGridSources = GladAlertsGridSources(gridId)

  def checkSources(gridId: String, windowExtent: Extent): GladAlertsGridSources = {
    val sources = getSources(gridId)
    checkRequired(sources.gladAlerts, windowExtent)
    checkRequired(sources.biomassPerHectar, windowExtent)
    checkOptional(sources.climateMask, windowExtent)

    sources
  }

  def getRasterSource(windowExtent: Extent): GladAlertsGridSources = {
    val gridId = GridId.pointGridId(windowExtent.center, gridSize)
    checkSources(gridId, windowExtent: Extent)
  }
}
