package org.globalforestwatch.summarystats.treecoverloss

case class TreeLossRow(
                        featureId: Int,
                        threshold: Int,
                        tcdYear: Int,
                        primaryForest: Boolean,
                        extent2000: Double,
                        extent2010: Double,
                        totalArea: Double,
                        totalGainArea: Double,
                        totalBiomass: Double,
                        totalCo2: Double,
                        meanBiomass: Option[Double],
                        yearData: List[TreeLossYearData])
