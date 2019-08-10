package org.globalforestwatch.annualupdate_minimal

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTenGrid

object TreeLossGrid extends TenByTenGrid[TreeLossGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String) = TreeLossGridSources.getCachedSources(gridId)


}
