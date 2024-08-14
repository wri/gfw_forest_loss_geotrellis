package org.globalforestwatch.summarystats.treecoverloss

case class TreeLossDataGroup(
                              threshold: Integer,
                              tcdYear: Int,
                              isPrimaryForest: Boolean,
                              isPlantations: Boolean,
                              isGlobalPeat: Boolean,
                              isTreeCoverLossFire: Boolean,
                              isGain: Boolean,
                              isUmdTreeCoverLoss: Boolean,
                              isIntactForestLandscapes2000: Boolean
                        )
