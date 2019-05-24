package org.globalforestwatch.treecoverloss

case class TreeLossRow(
                        lossRowFeatureId: TreeLossRowFeatureId,
                        threshold: Integer,
                        layers: TreeLossRowLayers,
                        extent2000: Double,
                        extent2010: Double,
                        totalArea: Double,
                        totalGainArea: Double,
                        totalBiomass: Double,
                        totalCo2: Double,
                        meanBiomass: Option[Double],
                        totalMangroveBiomass: Double,
                        totalMangroveCo2: Double,
                        meanMangroveBiomass: Option[Double],
                        yearData: List[TreeLossYearData])
