package org.globalforestwatch.summarystats.annualupdate_minimal

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen30mGrid}

object AnnualUpdateMinimalGrid extends TenByTen30mGrid[AnnualUpdateMinimalGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile, kwargs: Map[String, Any]) = AnnualUpdateMinimalGridSources.getCachedSources(gridTile, kwargs: Map[String, Any])


}
