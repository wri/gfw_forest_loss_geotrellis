package org.globalforestwatch.summarystats.carbonflux_custom_area

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTenGrid

object CarbonCustomGrid extends TenByTenGrid[CarbonCustomGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String, kwargs: Map[String, Any]) =
    CarbonCustomGridSources.getCachedSources(gridId)


}
