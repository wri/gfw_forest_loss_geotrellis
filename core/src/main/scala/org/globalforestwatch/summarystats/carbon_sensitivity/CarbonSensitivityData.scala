package org.globalforestwatch.summarystats.carbon_sensitivity

import cats.Semigroup

/** Summary data per class
 *
 * Note: This case class contains mutable values
 *
 */
case class CarbonSensitivityData(var totalTreecoverLoss: Double,
                                 var totalBiomassLoss: Double,
                                 var totalGrossEmissionsCo2eCo2Only: Double,
                                 var totalGrossEmissionsCo2eNonCo2: Double,
                                 var totalGrossEmissionsCo2e: Double,
                                 var totalAgcEmisYear: Double,
                                 var totalSoilCarbonEmisYear: Double,
                                 var totalTreecoverExtent2000: Double,
                                 var totalArea: Double,
                                 var totalBiomass: Double,
                                 var totalGrossCumulAbovegroundRemovalsCo2: Double,
                                 var totalGrossCumulBelowgroundRemovalsCo2: Double,
                                 var totalGrossCumulAboveBelowgroundRemovalsCo2: Double,
                                 var totalNetFluxCo2: Double,
                                 var totalJplTropicsAbovegroundBiomassDensity2000: Double,
                                 var totalTreecoverLossLegalAmazon: Double,
                                 var totalFluxModelExtentArea: Double
                                ) {
  def merge(other: CarbonSensitivityData): CarbonSensitivityData = {
    CarbonSensitivityData(
      totalTreecoverLoss + other.totalTreecoverLoss,
      totalBiomassLoss + other.totalBiomassLoss,
      totalGrossEmissionsCo2eCo2Only + other.totalGrossEmissionsCo2eCo2Only,
      totalGrossEmissionsCo2eNonCo2 + other.totalGrossEmissionsCo2eNonCo2,
      totalGrossEmissionsCo2e + other.totalGrossEmissionsCo2e,
      totalAgcEmisYear + other.totalAgcEmisYear,
      totalSoilCarbonEmisYear + other.totalSoilCarbonEmisYear,
      totalTreecoverExtent2000 + other.totalTreecoverExtent2000,
      totalArea + other.totalArea,
      totalBiomass + other.totalBiomass,
      totalGrossCumulAbovegroundRemovalsCo2 + other.totalGrossCumulAbovegroundRemovalsCo2,
      totalGrossCumulBelowgroundRemovalsCo2 + other.totalGrossCumulBelowgroundRemovalsCo2,
      totalGrossCumulAboveBelowgroundRemovalsCo2 + other.totalGrossCumulAboveBelowgroundRemovalsCo2,
      totalNetFluxCo2 + other.totalNetFluxCo2,
      totalJplTropicsAbovegroundBiomassDensity2000 + other.totalJplTropicsAbovegroundBiomassDensity2000,
      totalTreecoverLossLegalAmazon + other.totalTreecoverLossLegalAmazon,
      totalFluxModelExtentArea + other.totalFluxModelExtentArea
    )
  }
}

object CarbonSensitivityData {
  implicit val lossDataSemigroup: Semigroup[CarbonSensitivityData] =
    new Semigroup[CarbonSensitivityData] {
      def combine(x: CarbonSensitivityData, y: CarbonSensitivityData): CarbonSensitivityData =
        x.merge(y)
    }

}