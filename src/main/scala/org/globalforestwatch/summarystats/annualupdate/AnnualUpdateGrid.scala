package org.globalforestwatch.summarystats.annualupdate

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTenGrid

object AnnualUpdateGrid extends TenByTenGrid[AnnualUpdateGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String) =
    AnnualUpdateGridSources.getCachedSources(gridId)

}
