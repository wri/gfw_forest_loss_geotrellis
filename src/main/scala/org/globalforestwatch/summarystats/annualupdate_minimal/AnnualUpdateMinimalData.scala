package org.globalforestwatch.summarystats.annualupdate_minimal

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AnnualUpdateMinimalData(var areaLoss: Double,
                                   var biomassLoss: Double,
                                   var co2Emissions: Double,
                                   //                                   var mangroveBiomassLoss: Double,
                                   //                                   var mangroveCo2Emissions: Double,
                                   var extent2000: Double,
                                   var extent2010: Double,
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
      areaLoss + other.areaLoss,
      biomassLoss + other.biomassLoss,
      co2Emissions + other.co2Emissions,
      //      mangroveBiomassLoss + other.mangroveBiomassLoss,
      //      mangroveCo2Emissions + other.mangroveCo2Emissions,
      extent2000 + other.extent2000,
      extent2010 + other.extent2010,
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
