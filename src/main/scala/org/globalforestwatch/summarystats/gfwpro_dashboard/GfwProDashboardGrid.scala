package org.globalforestwatch.summarystats.gfwpro_dashboard

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen10mGrid, TenByTen30mGrid}

object GfwProDashboardGrid
  extends TenByTen30mGrid[GfwProDashboardGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile,
                 kwargs: Map[String, Any]): GfwProDashboardGridSources =
    GfwProDashboardGridSources.getCachedSources(gridTile, kwargs)

}
