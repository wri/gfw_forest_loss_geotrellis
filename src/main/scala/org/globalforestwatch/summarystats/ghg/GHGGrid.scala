package org.globalforestwatch.summarystats.ghg

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen30mGrid}

object GHGGrid
    extends TenByTen30mGrid[GHGGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile,
                 kwargs: Map[String, Any]): GHGGridSources =
    GHGGridSources.getCachedSources(gridTile, kwargs)

}
