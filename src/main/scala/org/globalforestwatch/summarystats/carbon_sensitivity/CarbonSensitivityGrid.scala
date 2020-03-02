package org.globalforestwatch.summarystats.carbon_sensitivity

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTen30mGrid

object CarbonSensitivityGrid extends TenByTen30mGrid[CarbonSensitivityGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String, kwargs: Map[String, Any]) =
    CarbonSensitivityGridSources.getCachedSources(gridId, kwargs)


}
