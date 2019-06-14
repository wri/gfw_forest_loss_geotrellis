package org.globalforestwatch.annualupdate_minimal

case class TreeLossYearData(year: Int,
                            var area_loss: Double,
                            var biomass_loss: Double,
                            var carbon_emissions: Double)

object TreeLossYearData {

  implicit object YearOrdering extends Ordering[TreeLossYearData] {
    def compare(a: TreeLossYearData, b: TreeLossYearData): Int =
      a.year compare b.year
  }

}
