package org.globalforestwatch.summarystats.annualupdate

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  * @param areaLoss
  * @param biomassLoss
  * @param co2Emissions
  * @param mangroveBiomassLoss
  * @param mangroveCo2Emissions
  * @param totalArea
  * @param totalGainArea
  * @param totalBiomass
  * @param totalCo2
  * @param weightedBiomass
  * @param totalMangroveBiomass
  * @param totalMangroveCo2
  * @param weightedMangroveBiomass
  */
case class AnnualUpdateData(var areaLoss: Double,
                            var biomassLoss: Double,
                            var co2Emissions: Double,
                            var mangroveBiomassLoss: Double,
                            var mangroveCo2Emissions: Double,
                            var extent2000: Double,
                            var extent2010: Double,
                            var totalArea: Double,
                            var totalGainArea: Double,
                            var totalBiomass: Double,
                            var totalCo2: Double,
                            var weightedBiomass: Double,
                            var totalMangroveBiomass: Double,
                            var totalMangroveCo2: Double,
                            var weightedMangroveBiomass: Double) {
  def merge(other: AnnualUpdateData): AnnualUpdateData = {

    AnnualUpdateData(
      areaLoss + other.areaLoss,
      biomassLoss + other.biomassLoss,
      co2Emissions + other.co2Emissions,
      mangroveBiomassLoss + other.mangroveBiomassLoss,
      mangroveCo2Emissions + other.mangroveCo2Emissions,
      extent2000 + other.extent2000,
      extent2010 + other.extent2010,
      totalArea + other.totalArea,
      totalGainArea + other.totalGainArea,
      totalBiomass + other.totalBiomass,
      totalCo2 + other.totalCo2,
      weightedBiomass + other.weightedBiomass,
      totalMangroveBiomass + other.totalMangroveBiomass,
      totalMangroveCo2 + other.totalMangroveBiomass,
      weightedMangroveBiomass + other.weightedMangroveBiomass
    )
  }
}

object AnnualUpdateData {
  implicit val lossDataSemigroup: Semigroup[AnnualUpdateData] =
    new Semigroup[AnnualUpdateData] {
      def combine(x: AnnualUpdateData, y: AnnualUpdateData): AnnualUpdateData =
        x.merge(y)
    }

}
