package org.globalforestwatch.annualupdate

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{TenByTenGrid, GridId}

object TreeLossGrid extends TenByTenGrid[TreeLossGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String) = TreeLossGridSources.getCachedSources(gridId)

}
