package org.globalforestwatch.treecoverloss

import cats.Semigroup
import geotrellis.raster.histogram.StreamingHistogram

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  * @param totalArea
  * @param totalGainArea
  * @param totalBiomass
  * @param totalCo2
  * @param biomassHistogram
  * @param totalMangroveBiomass
  * @param totalMangroveCo2
  * @param mangroveBiomassHistogram
  */
case class TreeLossData(
                         var lossYear: scala.collection.mutable.Map[Int, TreeLossYearData],
                         var extent2000: Double,
                         var extent2010: Double,
                         var totalArea: Double,
                         var totalGainArea: Double,
                         var totalBiomass: Double,
                         var totalCo2: Double,
                         var biomassHistogram: StreamingHistogram,
                         var totalMangroveBiomass: Double,
                         var totalMangroveCo2: Double,
                         var mangroveBiomassHistogram: StreamingHistogram
                       ) {
  def merge(other: TreeLossData): TreeLossData = {

    TreeLossData(
      lossYear ++ other.lossYear.map {
        case (k, v) => {
          val loss: TreeLossYearData = lossYear(k)
          var otherLoss: TreeLossYearData = v
          otherLoss.area_loss += loss.area_loss
          otherLoss.biomass_loss += loss.biomass_loss
          otherLoss.carbon_emissions += loss.carbon_emissions
          otherLoss.mangrove_biomass_loss += loss.mangrove_biomass_loss
          otherLoss.mangrove_carbon_emissions += loss.mangrove_carbon_emissions
          k -> otherLoss
        }
      },
      extent2000 + other.extent2000,
      extent2010 + other.extent2010,
      totalArea + other.totalArea,
      totalGainArea + other.totalGainArea,
      totalBiomass + other.totalBiomass,
      totalCo2 + other.totalCo2,
      biomassHistogram.merge(other.biomassHistogram),
      totalMangroveBiomass + other.totalMangroveBiomass,
      totalMangroveCo2 + other.totalMangroveBiomass,
      mangroveBiomassHistogram.merge(other.mangroveBiomassHistogram)
    )
  }
}

object TreeLossData {
  implicit val lossDataSemigroup: Semigroup[TreeLossData] =
    new Semigroup[TreeLossData] {
      def combine(x: TreeLossData, y: TreeLossData): TreeLossData = x.merge(y)
    }

}
