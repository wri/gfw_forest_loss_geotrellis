package org.globalforestwatch.summarystats.carbonflux

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTen30mGrid

object CarbonFluxGrid extends TenByTen30mGrid[CarbonFluxGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String, kwargs: Map[String, Any]) =
    CarbonFluxGridSources.getCachedSources(gridId)


}
