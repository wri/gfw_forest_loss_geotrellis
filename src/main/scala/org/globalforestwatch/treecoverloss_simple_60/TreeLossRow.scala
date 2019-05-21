package org.globalforestwatch.treecoverloss_simple_60

case class TreeLossRow(lossRowFeatureId: TreeLossRowFeatureId,
                       threshold: Integer,
                       extent2010: Double,
                       totalArea: Double,
                       yearData: List[TreeLossYearData])
