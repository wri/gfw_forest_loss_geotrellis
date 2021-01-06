package org.globalforestwatch.summarystats.carbonflux_minimal

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen30mGrid}

object CarbonFluxMinimalGrid extends TenByTen30mGrid[CarbonFluxMinimalGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile, kwargs: Map[String, Any]): CarbonFluxMinimalGridSources = CarbonFluxMinimalGridSources.getCachedSources(gridTile)


}
