package org.globalforestwatch.summarystats.gfwpro_dashboard_integrated

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen10mGrid, TenByTen30mGrid}

object GfwProDashboardGrid
  extends TenByTen10mGrid[GfwProDashboardGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile,
                 kwargs: Map[String, Any]): GfwProDashboardGridSources =
    GfwProDashboardGridSources.getCachedSources(gridTile, kwargs)

}
