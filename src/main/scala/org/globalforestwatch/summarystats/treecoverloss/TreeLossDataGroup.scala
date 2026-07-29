package org.globalforestwatch.summarystats.treecoverloss

// Must be in the same order as TreeLossDataGroup elsewhere, like in TreeLossSummary (ca. line 174)
case class TreeLossDataGroup(
                              threshold: Integer,
                              tcdYear: Int,
                              isPrimaryForest: Boolean,
                              isPlantations: Boolean,
                              isGlobalPeat: Boolean,
                              tclDriverClass: String,
                              isTreeCoverLossFire: Boolean,
                              isIFL2000: Boolean,
                              isUmdTreeCoverLoss: Boolean,
                              isGain: Boolean,
                              gpwCultivatedGrasslandExtent2020: Boolean,
                              gpwCultivatedGrasslandExtent2021: Boolean,
                              gpwCultivatedGrasslandExtent2022: Boolean,
                              gpwCultivatedGrasslandExtent2023: Boolean,
                              gpwCultivatedGrasslandExtent2024: Boolean
                        )
