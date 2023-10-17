package org.globalforestwatch.summarystats.forest_change_diagnostic

import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridTile, TenByTen30mGrid}

object ForestChangeDiagnosticGrid
    extends TenByTen30mGrid[ForestChangeDiagnosticGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridTile: GridTile,
                 kwargs: Map[String, Any]): ForestChangeDiagnosticGridSources =
    ForestChangeDiagnosticGridSources.getCachedSources(gridTile, kwargs)

}
