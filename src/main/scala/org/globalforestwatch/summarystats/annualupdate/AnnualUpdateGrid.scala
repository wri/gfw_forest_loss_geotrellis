package org.globalforestwatch.summarystats.annualupdate

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTen30mGrid

object AnnualUpdateGrid extends TenByTen30mGrid[AnnualUpdateGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String, kwargs: Map[String, Any]) =
    AnnualUpdateGridSources.getCachedSources(gridId)

}
