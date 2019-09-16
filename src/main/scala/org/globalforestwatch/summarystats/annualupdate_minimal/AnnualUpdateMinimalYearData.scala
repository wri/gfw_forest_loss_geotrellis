package org.globalforestwatch.summarystats.annualupdate_minimal

case class AnnualUpdateMinimalYearData(year: Int,
                                       var area_loss: Double,
                                       var biomass_loss: Double,
                                       var carbon_emissions: Double)

object AnnualUpdateMinimalYearData {

  implicit object YearOrdering extends Ordering[AnnualUpdateMinimalYearData] {
    def compare(a: AnnualUpdateMinimalYearData, b: AnnualUpdateMinimalYearData): Int =
      a.year compare b.year
  }

}
