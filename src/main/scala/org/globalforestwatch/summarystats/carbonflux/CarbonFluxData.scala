package org.globalforestwatch.summarystats.carbonflux

import cats.Semigroup

/** Summary data per class
 *
 * Note: This case class contains mutable values
 *
 */
case class CarbonFluxData(var totalTreecoverLoss: Double,
                          var totalBiomassLoss: Double,
                          var totalGrossEmissionsCo2eCo2Only: Double,
                          var totalGrossEmissionsCo2eNonCo2: Double,
                          var totalGrossEmissionsCo2e: Double,
                          var totalAgcEmisYear: Double,
                          var totalBgcEmisYear: Double,
                          var totalDeadwoodCarbonEmisYear: Double,
                          var totalLitterCarbonEmisYear: Double,
                          var totalSoilCarbonEmisYear: Double,
                          var totalCarbonEmisYear: Double,
                          var totalTreecoverExtent2000: Double,
                          var totalArea: Double,
                          var totalBiomass: Double,
                          var totalGrossAnnualAbovegroundRemovalsCarbon: Double,
                          var totalGrossAnnualBelowgroundRemovalsCarbon: Double,
                          var totalGrossAnnualAboveBelowgroundRemovalsCarbon: Double,
                          var totalGrossCumulAbovegroundRemovalsCo2: Double,
                          var totalGrossCumulBelowgroundRemovalsCo2: Double,
                          var totalGrossCumulAboveBelowgroundRemovalsCo2: Double,
                          var totalNetFluxCo2: Double,
                          var totalAgc2000: Double,
                          var totalBgc2000: Double,
                          var totalDeadwoodCarbon2000: Double,
                          var totalLitterCarbon2000: Double,
                          var totalSoilCarbon2000: Double,
                          var totalCarbon2000: Double,
                          var totalJplTropicsAbovegroundBiomassDensity2000: Double,
                          var totalTreecoverLossLegalAmazon: Double,
                          var totalStdevAnnualAbovegroundRemovalsCarbon: Double,
                          var totalStdevSoilCarbonEmisYear: Double
                         ) {
  def merge(other: CarbonFluxData): CarbonFluxData = {
    CarbonFluxData(
      totalTreecoverLoss + other.totalTreecoverLoss,
      totalBiomassLoss + other.totalBiomassLoss,
      totalGrossEmissionsCo2eCo2Only + other.totalGrossEmissionsCo2eCo2Only,
      totalGrossEmissionsCo2eNonCo2 + other.totalGrossEmissionsCo2eNonCo2,
      totalGrossEmissionsCo2e + other.totalGrossEmissionsCo2e,
      totalAgcEmisYear + other.totalAgcEmisYear,
      totalBgcEmisYear + other.totalBgcEmisYear,
      totalDeadwoodCarbonEmisYear + other.totalDeadwoodCarbonEmisYear,
      totalLitterCarbonEmisYear + other.totalLitterCarbonEmisYear,
      totalSoilCarbonEmisYear + other.totalSoilCarbonEmisYear,
      totalCarbonEmisYear + other.totalCarbonEmisYear,
      totalTreecoverExtent2000 + other.totalTreecoverExtent2000,
      totalArea + other.totalArea,
      totalBiomass + other.totalBiomass,
      totalGrossAnnualAbovegroundRemovalsCarbon + other.totalGrossAnnualAbovegroundRemovalsCarbon,
      totalGrossAnnualBelowgroundRemovalsCarbon + other.totalGrossAnnualBelowgroundRemovalsCarbon,
      totalGrossAnnualAboveBelowgroundRemovalsCarbon + other.totalGrossAnnualAboveBelowgroundRemovalsCarbon,
      totalGrossCumulAbovegroundRemovalsCo2 + other.totalGrossCumulAbovegroundRemovalsCo2,
      totalGrossCumulBelowgroundRemovalsCo2 + other.totalGrossCumulBelowgroundRemovalsCo2,
      totalGrossCumulAboveBelowgroundRemovalsCo2 + other.totalGrossCumulAboveBelowgroundRemovalsCo2,
      totalNetFluxCo2 + other.totalNetFluxCo2,
      totalAgc2000 + other.totalAgc2000,
      totalBgc2000 + other.totalBgc2000,
      totalDeadwoodCarbon2000 + other.totalDeadwoodCarbon2000,
      totalLitterCarbon2000 + other.totalLitterCarbon2000,
      totalSoilCarbon2000 + other.totalSoilCarbon2000,
      totalCarbon2000 + other.totalCarbon2000,
      totalJplTropicsAbovegroundBiomassDensity2000 + other.totalJplTropicsAbovegroundBiomassDensity2000,
      totalTreecoverLossLegalAmazon + other.totalTreecoverLossLegalAmazon,
      totalStdevAnnualAbovegroundRemovalsCarbon + other.totalStdevAnnualAbovegroundRemovalsCarbon,
      totalStdevSoilCarbonEmisYear + other.totalStdevSoilCarbonEmisYear
    )
  }
}

object CarbonFluxData {
  implicit val lossDataSemigroup: Semigroup[CarbonFluxData] =
    new Semigroup[CarbonFluxData] {
      def combine(x: CarbonFluxData, y: CarbonFluxData): CarbonFluxData =
        x.merge(y)
    }

}
