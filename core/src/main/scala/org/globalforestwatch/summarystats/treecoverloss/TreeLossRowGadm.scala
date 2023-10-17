package org.globalforestwatch.summarystats.treecoverloss

import org.globalforestwatch.features.GadmFeatureId

case class TreeLossRowGadm(id: GadmFeatureId,
                           dataGroup: TreeLossDataGroup,
                           data: TreeLossData)
