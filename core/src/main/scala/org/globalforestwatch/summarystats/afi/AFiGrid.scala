package org.globalforestwatch.summarystats.afi

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen30mGrid}

object AFiGrid
  extends TenByTen30mGrid[AFiGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile,
                 kwargs: Map[String, Any]): AFiGridSources =
    AFiGridSources.getCachedSources(gridTile, kwargs)

}
