package org.globalforestwatch.summarystats.treecoverloss

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen30mGrid}

object TreeLossGrid extends TenByTen30mGrid[TreeLossGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile, kwargs: Map[String, Any]): TreeLossGridSources = TreeLossGridSources.getCachedSources(gridTile)


}
