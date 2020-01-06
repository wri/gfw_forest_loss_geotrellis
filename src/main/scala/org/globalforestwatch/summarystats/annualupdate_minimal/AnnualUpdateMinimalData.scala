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
                                   var totalCo2: Double
                                   //                                   var totalMangroveBiomass: Double,
                                   //                                   var totalMangroveCo2: Double,
                                   //                                   var weightedMangroveBiomass: Double
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
      totalCo2 + other.totalCo2
//      totalMangroveBiomass + other.totalMangroveBiomass,
//      totalMangroveCo2 + other.totalMangroveBiomass,
//      mangroveBiomassHistogram.merge(other.mangroveBiomassHistogram)
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
