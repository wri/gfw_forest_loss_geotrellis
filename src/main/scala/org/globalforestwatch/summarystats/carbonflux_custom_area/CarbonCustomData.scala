package org.globalforestwatch.summarystats.carbonflux_custom_area

import cats.Semigroup

/** Summary data per class
 *
 * Note: This case class contains mutable values
 *
 */
case class CarbonCustomData(var totalTreecoverLoss: Double,
                            var totalBiomassLoss: Double,
                            var totalGrossEmissionsCo2eCo2Only: Double,
                            var totalGrossEmissionsCo2eNoneCo2: Double,
                            var totalGrossEmissionsCo2e: Double,
                            var totalTreecoverExtent2000: Double,
                            var totalArea: Double,
                            var totalBiomass: Double,
                            var totalGrossAnnualRemovalsCarbon: Double,
                            var totalGrossCumulRemovalsCarbon: Double,
                            var totalNetFluxCo2: Double
                         ) {
  def merge(other: CarbonCustomData): CarbonCustomData = {
    CarbonCustomData(
      totalTreecoverLoss + other.totalTreecoverLoss,
      totalBiomassLoss + other.totalBiomassLoss,
      totalGrossEmissionsCo2eCo2Only + other.totalGrossEmissionsCo2eCo2Only,
      totalGrossEmissionsCo2eNoneCo2 + other.totalGrossEmissionsCo2eNoneCo2,
      totalGrossEmissionsCo2e + other.totalGrossEmissionsCo2e,
      totalTreecoverExtent2000 + other.totalTreecoverExtent2000,
      totalArea + other.totalArea,
      totalBiomass + other.totalBiomass,
      totalGrossAnnualRemovalsCarbon + other.totalGrossAnnualRemovalsCarbon,
      totalGrossCumulRemovalsCarbon + other.totalGrossCumulRemovalsCarbon,
      totalNetFluxCo2 + other.totalNetFluxCo2
    )
  }
}

object CarbonCustomData {
  implicit val lossDataSemigroup: Semigroup[CarbonCustomData] =
    new Semigroup[CarbonCustomData] {
      def combine(x: CarbonCustomData, y: CarbonCustomData): CarbonCustomData =
        x.merge(y)
    }

}
