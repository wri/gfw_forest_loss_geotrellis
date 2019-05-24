package org.globalforestwatch.treecoverloss_simple_60

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTenGrid

object TreeLossGrid extends TenByTenGrid[TreeLossGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String) = TreeLossGridSources.getCachedSources(gridId)

  def checkSources(gridId: String,
                   windowExtent: Extent): TreeLossGridSources = {

    val sources: TreeLossGridSources = getSources(gridId)

    checkRequired(sources.treeCoverLoss, windowExtent)
    checkRequired(sources.treeCoverDensity2010_60, windowExtent)

    sources

  }

}
