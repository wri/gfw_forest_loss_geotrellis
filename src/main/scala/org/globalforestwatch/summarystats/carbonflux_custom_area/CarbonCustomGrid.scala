package org.globalforestwatch.summarystats.carbonflux_custom_area

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen30mGrid}

object CarbonCustomGrid extends TenByTen30mGrid[CarbonCustomGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile, kwargs: Map[String, Any]) =
    CarbonCustomGridSources.getCachedSources(gridTile)


}
