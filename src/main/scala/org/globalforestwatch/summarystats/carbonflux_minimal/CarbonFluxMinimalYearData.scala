package org.globalforestwatch.summarystats.carbonflux_minimal

case class CarbonFluxMinimalYearData(year: Int,
                                     var treecoverLoss: Double,
                                     var biomassLoss: Double,
                                     var grossEmissionsCo2eCo2Only: Double,
                                     var grossEmissionsCo2eNonCo2: Double,
                                     var grossEmissionsCo2eAllGases: Double
                           )

object CarbonFluxMinimalYearData {

  implicit object YearOrdering extends Ordering[CarbonFluxMinimalYearData] {
    def compare(a: CarbonFluxMinimalYearData, b: CarbonFluxMinimalYearData): Int = a.year compare b.year
  }

}
