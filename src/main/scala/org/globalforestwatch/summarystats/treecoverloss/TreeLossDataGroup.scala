package org.globalforestwatch.summarystats.treecoverloss

case class TreeLossDataGroup(
                              threshold: Integer,
                              tcdYear: Int,
                              isPrimaryForest: Boolean,
                              isPlantations: Boolean,
                              isGlobalPeat: Boolean,
                              tclDriverClass: String,
                              isTreeCoverLossFire: Boolean,
                              isGain: Boolean
                        )
