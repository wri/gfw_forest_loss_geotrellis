package org.globalforestwatch.summarystats.ghg

import scala.collection.immutable.SortedMap

case class GHGRawDataGroup(umdTreeCoverLossYear: Int,
                           cropYield: Double
) {

  /** Produce a partial GHGData for the loss year, yield, emissions, and area in this
    * data group */
  def toGHGData(totalArea: Double, emissionsCo2e: Double): GHGData = {
    val minLossYear = GHGCommand.GHGYearStart
    val maxLossYear = GHGCommand.GHGYearEnd

    // Emissions factor formula:
    //
    // 20-year discounted emissions for year 2020 (tonsCO2e) =
    //   (co2e emissions 2020 * 0.0975) + (co2e emissions 2019 * 0.0925) + (co2e emissions 2018 * 0.0875) + ... + (co2e emissions 2001 * 0.025)
    // production (kg) = yield * area
    // emissions factor (tonsCO2e/kg) = discounted emissions / production
    
    val efList = for (i <- minLossYear to maxLossYear) yield {
      val diff = i - umdTreeCoverLossYear
      if (diff >= 0 && diff < 20) {
        (i -> ((0.0975 - diff * 0.005) * emissionsCo2e) / (cropYield * totalArea))
      } else {
        (i -> 0.0)
      }
    }

    val r = GHGData(
      total_area = GHGDataDouble.fill(totalArea),
      emissions_factor_yearly = GHGDataValueYearly(SortedMap(efList.toSeq: _*))
    )
    r
  }
}
