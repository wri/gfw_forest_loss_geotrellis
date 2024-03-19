package org.globalforestwatch.summarystats.afi

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen10mGrid}

object AFiGrid
  // Using 10m grid, since the native resolution of JRC map is around 10m.
  extends TenByTen10mGrid[AFiGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile,
                 kwargs: Map[String, Any]): AFiGridSources =
    AFiGridSources.getCachedSources(gridTile, kwargs)

}
