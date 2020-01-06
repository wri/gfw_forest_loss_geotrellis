package org.globalforestwatch.summarystats.annualupdate_minimal

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTenGrid

object AnnualUpdateMinimalGrid extends TenByTenGrid[AnnualUpdateMinimalGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String, kwargs: Map[String, Any]) = AnnualUpdateMinimalGridSources.getCachedSources(gridId)


}
