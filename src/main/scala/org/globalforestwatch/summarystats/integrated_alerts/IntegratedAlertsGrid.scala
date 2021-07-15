package org.globalforestwatch.summarystats.integrated_alerts

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen10mGrid}

object IntegratedAlertsGrid extends TenByTen10mGrid[IntegratedAlertsGridSources] {

  val gridExtent = Extent(-180.0000, -30.0000, 180.0000, 30.0000)

  def getSources(gridTile: GridTile, kwargs: Map[String, Any]): IntegratedAlertsGridSources = IntegratedAlertsGridSources.getCachedSources(gridTile)


}
