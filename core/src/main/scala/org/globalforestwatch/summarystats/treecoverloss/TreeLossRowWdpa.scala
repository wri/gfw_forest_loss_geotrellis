package org.globalforestwatch.summarystats.treecoverloss

import org.globalforestwatch.features.WdpaFeatureId

case class TreeLossRowWdpa(id: WdpaFeatureId,
                           dataGroup: TreeLossDataGroup,
                           data: TreeLossData)
