package org.globalforestwatch.summarystats.treecoverloss

import org.globalforestwatch.features.SimpleFeatureId

case class TreeLossRowSimple(id: SimpleFeatureId,
                             dataGroup: TreeLossDataGroup,
                             data: TreeLossData)
