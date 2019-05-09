package org.globalforestwatch.gladalerts

import geotrellis.vector.Extent
import org.globalforestwatch.layers._
import org.globalforestwatch.util.TenByTenGrid

object GladAlertsGrid extends TenByTenGrid {

  def getRasterSource(windowExtent: Extent): GladAlertsGridSources = {
    val gridId = pointGridId(windowExtent.center)
    val sources = GladAlertsGridSources(gridId)

    // NOTE: This check will cause an eager fetch of raster metadata
    def checkRequired(layer: RequiredLayer): Unit = {
      require(
        layer.source.extent.intersects(windowExtent),
        s"${layer.uri} does not intersect: $windowExtent"
      )
    }

    // Only check these guys if they're defined
    def checkOptional(layer: OptionalLayer): Unit = {
      layer.source.foreach { source =>
        require(
          source.extent.intersects(windowExtent),
          s"${source.uri} does not intersect: $windowExtent"
        )
      }
    }

    checkRequired(sources.gladAlerts)
    checkRequired(sources.biomassPerHectar)
    checkOptional(sources.climateMask)

    sources
  }
}
