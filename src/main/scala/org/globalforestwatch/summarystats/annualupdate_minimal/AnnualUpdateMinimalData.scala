package org.globalforestwatch.summarystats.annualupdate_minimal

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AnnualUpdateMinimalData(var treecoverLoss: Double,
                                   var biomassLoss: Double,
                                   var co2Emissions: Double,
                                   //                                   var mangroveBiomassLoss: Double,
                                   //                                   var mangroveCo2Emissions: Double,
                                   var treecoverExtent2000: Double,
                                   var treecoverExtent2010: Double,
                                   var totalArea: Double,
                                   var totalGainArea: Double,
                                   var totalBiomass: Double,
                                   var totalGrossEmissionsCo2eCo2Only: Double,
                                   var totalGrossEmissionsCo2eNonCo2: Double,
                                   var totalGrossEmissionsCo2e: Double,
                                   var totalGrossCumulAbovegroundRemovalsCo2: Double,
                                   var totalGrossCumulBelowgroundRemovalsCo2: Double,
                                   var totalGrossCumulAboveBelowgroundRemovalsCo2: Double,
                                   var totalNetFluxCo2: Double,
                                   var totalCo2: Double,
                                   var totalSoilCarbon: Double
                                  ) {
  def merge(other: AnnualUpdateMinimalData): AnnualUpdateMinimalData = {
    AnnualUpdateMinimalData(
      treecoverLoss + other.treecoverLoss,
      biomassLoss + other.biomassLoss,
      co2Emissions + other.co2Emissions,
      //      mangroveBiomassLoss + other.mangroveBiomassLoss,
      //      mangroveCo2Emissions + other.mangroveCo2Emissions,
      treecoverExtent2000 + other.treecoverExtent2000,
      treecoverExtent2010 + other.treecoverExtent2010,
      totalArea + other.totalArea,
      totalGainArea + other.totalGainArea,
      totalBiomass + other.totalBiomass,
      totalGrossEmissionsCo2eCo2Only + other.totalGrossEmissionsCo2eCo2Only,
      totalGrossEmissionsCo2eNonCo2 + other.totalGrossEmissionsCo2eNonCo2,
      totalGrossEmissionsCo2e + other.totalGrossEmissionsCo2e,
      totalGrossCumulAbovegroundRemovalsCo2 + other.totalGrossCumulAbovegroundRemovalsCo2,
      totalGrossCumulBelowgroundRemovalsCo2 + other.totalGrossCumulBelowgroundRemovalsCo2,
      totalGrossCumulAboveBelowgroundRemovalsCo2 + other.totalGrossCumulAboveBelowgroundRemovalsCo2,
      totalNetFluxCo2 + other.totalNetFluxCo2,
      totalCo2 + other.totalCo2,
      totalSoilCarbon + other.totalSoilCarbon
    )
  }
}

object AnnualUpdateMinimalData {
  implicit val lossDataSemigroup: Semigroup[AnnualUpdateMinimalData] =
    new Semigroup[AnnualUpdateMinimalData] {
      def combine(x: AnnualUpdateMinimalData,
                  y: AnnualUpdateMinimalData): AnnualUpdateMinimalData =
        x.merge(y)
    }

}
