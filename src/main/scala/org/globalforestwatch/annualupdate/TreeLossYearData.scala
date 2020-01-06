package org.globalforestwatch.annualupdate

case class TreeLossYearData(year: Int,
                            var area_loss: Double,
                            var biomass_loss: Double,
                            var carbon_emissions: Double,
                            var mangrove_biomass_loss: Double,
                            var mangrove_carbon_emissions: Double)

object TreeLossYearData {

  implicit object YearOrdering extends Ordering[TreeLossYearData] {
    def compare(a: TreeLossYearData, b: TreeLossYearData): Int =
      a.year compare b.year
  }

}
