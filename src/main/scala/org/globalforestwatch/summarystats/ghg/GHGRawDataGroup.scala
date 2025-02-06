package org.globalforestwatch.summarystats.ghg

import scala.collection.immutable.SortedMap

case class GHGRawDataGroup(umdTreeCoverLossYear: Int,
                           cropYield: Double
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
    // production (kg) = yield * area
    // emissions factor (tonsCO2e/kg) = discounted emissions / production
    
    def emissionsMap(emissions: Double): SortedMap[Int, Double] = {
      val r = for (i <- minLossYear to maxLossYear) yield {
        val diff = i - umdTreeCoverLossYear
        if (diff >= 0 && diff < DiscountNumberOfYears) {
          (i -> ((DiscountStartValue - diff * DiscountDecreasePerYear) * emissions) / (cropYield * totalArea))
        } else {
          (i -> 0.0)
        }
      }
      SortedMap(r.toSeq: _*)
    }

    val r = GHGData(
      total_area = GHGDataDouble.fill(totalArea),
      ef_co2_yearly = GHGDataValueYearly(emissionsMap(emissionsCo2eCO2)),
      ef_ch4_yearly = GHGDataValueYearly(emissionsMap(emissionsCo2eCH4)),
      ef_n2o_yearly = GHGDataValueYearly(emissionsMap(emissionsCo2eN2O)),
      emissions_factor_yearly = GHGDataValueYearly(emissionsMap(emissionsCo2e))
    )
    r
  }
}
