package org.globalforestwatch.summarystats.treecoverloss


case class TreeLossYearData(year: Int,
                            var treecoverLoss: Double,
                            var biomassLoss: Double,
                            var grossEmissionsCo2eCo2Only: Double,
                            var grossEmissionsCo2eNonCo2: Double,
                            var grossEmissionsCo2eAllGases: Double
                           )

object TreeLossYearData {

  implicit object YearOrdering extends Ordering[TreeLossYearData] {
    def compare(a: TreeLossYearData, b: TreeLossYearData): Int = a.year compare b.year
  }

}
