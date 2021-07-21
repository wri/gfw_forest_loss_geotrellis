package org.globalforestwatch.summarystats.firealerts

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, NinetyByNinety1kmGrid, TenByTen30mGrid}

object ModisGrid extends NinetyByNinety1kmGrid[FireAlertsGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile, kwargs: Map[String, Any]): FireAlertsGridSources = FireAlertsGridSources.getCachedSources(gridTile, kwargs)

}
