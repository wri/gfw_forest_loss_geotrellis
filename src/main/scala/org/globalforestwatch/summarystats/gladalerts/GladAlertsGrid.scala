package org.globalforestwatch.summarystats.gladalerts

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridId, TenByTenGrid}

object GladAlertsGrid extends TenByTenGrid[GladAlertsGridSources] {

  val gridExtent = Extent(-180.0000, -30.0000, 180.0000, 30.0000)

  def getSources(gridId: String, kwargs: Map[String, Any]): GladAlertsGridSources = GladAlertsGridSources.getCachedSources(gridId)


}
