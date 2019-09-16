package org.globalforestwatch.summarystats.carbonflux

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTenGrid

object CarbonFluxGrid extends TenByTenGrid[CarbonFluxGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String) =
    CarbonFluxGridSources.getCachedSources(gridId)


}
