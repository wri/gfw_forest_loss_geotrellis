package org.globalforestwatch.summarystats.annualupdate

case class AnnualUpdateYearData(year: Int,
                                var area_loss: Double,
                                var biomass_loss: Double,
                                var carbon_emissions: Double,
                                var mangrove_biomass_loss: Double,
                                var mangrove_carbon_emissions: Double)

object AnnualUpdateYearData {

  implicit object YearOrdering extends Ordering[AnnualUpdateYearData] {
    def compare(a: AnnualUpdateYearData, b: AnnualUpdateYearData): Int =
      a.year compare b.year
  }

}
