package org.globalforestwatch.summarystats.ghg

import scala.collection.immutable.SortedMap

case class GHGRawDataGroup(umdTreeCoverLossYear: Int,
                           cropYield: Double // kg/hectare
) {
  val DiscountNumberOfYears = 20
  val DiscountStartValue = 0.0975
  val DiscountDecreasePerYear = 0.005

  /** Produce a partial GHGData for the loss year, yield, emissions, and area in this
    * data group */
  def toGHGData(totalArea: Double, emissionsCo2eCO2: Double, emissionsCo2eCH4: Double, emissionsCo2eN2O: Double, emissionsCo2e: Double): GHGData = {
    val minLossYear = GHGCommand.GHGYearStart
    val maxLossYear = GHGCommand.GHGYearEnd

    // Emissions factor formula:
    //
    // 20-year discounted emissions for year 2020 (tonsCO2e) =
    //   (co2e emissions 2020 * 0.0975) + (co2e emissions 2019 * 0.0925) + (co2e emissions 2018 * 0.0875) + ... + (co2e emissions 2001 * 0.025)
    // production (tons) = yield * area / 1000
    // emissions factor (tonsCO2e/tons yield) = discounted emissions / production

    // The production is counted over all areas, including areas with no emissions
    // (no tree cover loss), so we don't divide by production until all data is
    // merged.

    def emissionsMap(emissions: Double): SortedMap[Int, Double] = {
      val r = for (i <- minLossYear to maxLossYear) yield {
        val diff = i - umdTreeCoverLossYear
        if (diff >= 0 && diff < DiscountNumberOfYears) {
          (i -> ((DiscountStartValue - diff * DiscountDecreasePerYear) * emissions))
        } else {
          (i -> 0.0)
        }
      }
      SortedMap(r.toSeq: _*)
    }

    println(s"area $totalArea, cropYield $cropYield, tcl year $umdTreeCoverLossYear, emissions $emissionsCo2e")
    val r = GHGData(
      total_area = GHGDataDouble.fill(totalArea),
      // Extra divide by 1000, so production is in Mg (metric tonnes)
      production = GHGDataDouble.fill(totalArea * cropYield / 1000),
      ef_co2_yearly = GHGDataValueYearly(emissionsMap(emissionsCo2eCO2)),
      ef_ch4_yearly = GHGDataValueYearly(emissionsMap(emissionsCo2eCH4)),
      ef_n2o_yearly = GHGDataValueYearly(emissionsMap(emissionsCo2eN2O)),
      emissions_factor_yearly = GHGDataValueYearly(emissionsMap(emissionsCo2e))
    )
    r
  }
}
