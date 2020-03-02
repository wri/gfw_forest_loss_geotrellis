package org.globalforestwatch.summarystats.firealerts

import geotrellis.vector.Extent
import org.globalforestwatch.grids.EightByEight1kmGrid

object ModisGrid extends EightByEight1kmGrid[FireAlertsGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String, kwargs: Map[String, Any]): FireAlertsGridSources = FireAlertsGridSources.getCachedSources(gridId)

}
