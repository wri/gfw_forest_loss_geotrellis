package org.globalforestwatch.summarystats.carbon_sensitivity

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  */
case class CarbonSensitivityData(var treecoverLoss: Double,
                          var biomassLoss: Double,
                          var grossEmissionsCo2eCo2Only: Double,
                          var grossEmissionsCo2eNoneCo2: Double,
                          var grossEmissionsCo2e: Double,
                          var agcEmisYear: Double,
//                          var bgcEmisYear: Double,
//                          var deadwoodCarbonEmisYear: Double,
//                          var litterCarbonEmisYear: Double,
                          var soilCarbonEmisYear: Double,
//                          var carbonEmisYear: Double,
                          var treecoverExtent2000: Double,
                          var totalArea: Double,
                          var totalBiomass: Double,
//                          var totalGrossAnnualRemovalsCarbon: Double,
                          var totalGrossCumulRemovalsCarbon: Double,
                          var totalNetFluxCo2: Double,
                          var totalAgc2000: Double,
//                          var totalBgc2000: Double,
//                          var totalDeadwoodCarbon2000: Double,
//                          var totalLitterCarbon2000: Double,
                          var totalSoil2000: Double
//                          var totalCarbon2000: Double
                          //                          var totalGrossEmissionsCo2eCo2Only: Double,
                          //                          var totalGrossEmissionsCo2eNoneCo2: Double,
                          //                          var totalGrossEmissionsCo2e: Double
) {
  def merge(other: CarbonSensitivityData): CarbonSensitivityData = {
    CarbonSensitivityData(
      treecoverLoss + other.treecoverLoss,
      biomassLoss + other.biomassLoss,
      grossEmissionsCo2eCo2Only + other.grossEmissionsCo2eCo2Only,
      grossEmissionsCo2eNoneCo2 + other.grossEmissionsCo2eNoneCo2,
      grossEmissionsCo2e + other.grossEmissionsCo2e,
      agcEmisYear + other.agcEmisYear,
//      bgcEmisYear + other.bgcEmisYear,
//      deadwoodCarbonEmisYear + other.deadwoodCarbonEmisYear,
//      litterCarbonEmisYear + other.litterCarbonEmisYear,
      soilCarbonEmisYear + other.soilCarbonEmisYear,
//      carbonEmisYear + other.carbonEmisYear,
      treecoverExtent2000 + other.treecoverExtent2000,
      totalArea + other.totalArea,
      totalBiomass + other.totalBiomass,
//      totalGrossAnnualRemovalsCarbon + other.totalGrossAnnualRemovalsCarbon,
      totalGrossCumulRemovalsCarbon + other.totalGrossCumulRemovalsCarbon,
      totalNetFluxCo2 + other.totalNetFluxCo2,
      totalAgc2000 + other.totalAgc2000,
//      totalBgc2000 + other.totalBgc2000,
//      totalDeadwoodCarbon2000 + other.totalDeadwoodCarbon2000,
//      totalLitterCarbon2000 + other.totalLitterCarbon2000,
      totalSoil2000 + other.totalSoil2000
//      totalCarbon2000 + other.totalCarbon2000
      //      totalGrossEmissionsCo2eCo2Only + other.totalGrossEmissionsCo2eCo2Only,
      //      totalGrossEmissionsCo2eNoneCo2 + other.totalGrossEmissionsCo2eNoneCo2,
      //      totalGrossEmissionsCo2e + other.totalGrossEmissionsCo2e
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
