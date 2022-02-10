package org.globalforestwatch.summarystats.gladalerts

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen30mGrid}

object GladAlertsGrid extends TenByTen30mGrid[GladAlertsGridSources] {

  val gridExtent = Extent(-180.0000, -30.0000, 180.0000, 30.0000)

  def getSources(gridTile: GridTile, kwargs: Map[String, Any]): GladAlertsGridSources = GladAlertsGridSources.getCachedSources(gridTile)


}
