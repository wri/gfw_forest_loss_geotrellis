package org.globalforestwatch.summarystats.carbonflux

case class CarbonFluxYearData(year: Int,
                              var area_loss: Double,
                              var biomass_loss: Double,
                              var gross_emissions_co2: Double)

object CarbonFluxYearData {

  implicit object YearOrdering extends Ordering[CarbonFluxYearData] {
    def compare(a: CarbonFluxYearData, b: CarbonFluxYearData): Int =
      a.year compare b.year
  }

}
