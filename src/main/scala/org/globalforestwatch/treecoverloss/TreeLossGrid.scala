package org.globalforestwatch.treecoverloss

import geotrellis.vector.Extent
import org.globalforestwatch.grids.TenByTenGrid

object TreeLossGrid extends TenByTenGrid[TreeLossGridSources] {

  val gridExtent: Extent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)

  def getSources(gridId: String): TreeLossGridSources = TreeLossGridSources.getCachedSources(gridId)

  def checkSources(gridId: String,
                   windowExtent: Extent): TreeLossGridSources = {

    val sources: TreeLossGridSources = getSources(gridId)

    checkRequired(sources.treeCoverLoss, windowExtent)
    checkRequired(sources.treeCoverGain, windowExtent)
    checkRequired(sources.treeCoverDensity2000, windowExtent)
    checkRequired(sources.treeCoverDensity2010, windowExtent)
    checkOptional(sources.biomassPerHectar, windowExtent)
    checkOptional(sources.primaryForest, windowExtent)
    sources

  }

}
