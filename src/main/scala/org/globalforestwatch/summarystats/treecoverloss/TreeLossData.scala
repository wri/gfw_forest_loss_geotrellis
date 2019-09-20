package org.globalforestwatch.summarystats.treecoverloss

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class TreeLossData(
                         var lossYear: scala.collection.mutable.Map[Int, TreeLossYearData],
                         var treecoverExtent2000: Double,
                         var treecoverExtent2010: Double,
                         var totalArea: Double,
                         var totalGainArea: Double,
                         var totalBiomass: Double,
                         var totalCo2: Double,
                         var avgBiomass: Double
                       ) {
  def merge(other: TreeLossData): TreeLossData = {

    TreeLossData(
      lossYear ++ other.lossYear.map {
        case (k, v) => {
          val loss: TreeLossYearData = lossYear(k)
          var otherLoss: TreeLossYearData = v
          otherLoss.treecoverLoss += loss.treecoverLoss
          otherLoss.biomassLoss += loss.biomassLoss
          otherLoss.carbonEmissions += loss.carbonEmissions
          k -> otherLoss
        }
      },
      treecoverExtent2000 + other.treecoverExtent2000,
      treecoverExtent2010 + other.treecoverExtent2010,
      totalArea + other.totalArea,
      totalGainArea + other.totalGainArea,
      totalBiomass + other.totalBiomass,
      totalCo2 + other.totalCo2,
      // TODO: use extent2010 to calculate avg biomass incase year is selected
      ((avgBiomass * treecoverExtent2000) + (other.avgBiomass * other.treecoverExtent2000)) / (treecoverExtent2000 + other.treecoverExtent2000)
    )
  }
}

object TreeLossData {
  implicit val lossDataSemigroup: Semigroup[TreeLossData] =
    new Semigroup[TreeLossData] {
      def combine(x: TreeLossData, y: TreeLossData): TreeLossData = x.merge(y)
    }

}
