package org.globalforestwatch.summarystats.treecoverloss


case class TreeLossYearData(year: Int,
                            var treecoverLoss: Double,
                            var primaryLoss: Double,
                            var wdpaLoss: Double,
                            var peatlandsLoss: Double,
                            var iflLoss: Double
                           )

object TreeLossYearData {

  implicit object YearOrdering extends Ordering[TreeLossYearData] {
    def compare(a: TreeLossYearData, b: TreeLossYearData): Int = a.year compare b.year
  }

}
