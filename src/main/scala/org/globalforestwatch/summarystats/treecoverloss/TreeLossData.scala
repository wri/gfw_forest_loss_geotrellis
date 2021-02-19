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
                         var avgBiomass: Double,
                         var totalGrossCumulAbovegroundRemovalsCo2: Double,
                         var totalGrossCumulBelowgroundRemovalsCo2: Double,
                         var totalGrossCumulAboveBelowgroundRemovalsCo2: Double,
                         var totalGrossEmissionsCo2eCo2Only: Double,
                         var totalGrossEmissionsCo2eNonCo2: Double,
                         var totalGrossEmissionsCo2eAllGases: Double,
                         var totalNetFluxCo2: Double,
                         var totalFluxModelExtentArea: Double
                       ) {
  def merge(other: TreeLossData): TreeLossData = {

    TreeLossData(
      lossYear ++ other.lossYear.map {
        case (k, v) => {
          val loss: TreeLossYearData = lossYear(k)
          val otherLoss: TreeLossYearData = v
          otherLoss.treecoverLoss += loss.treecoverLoss
          otherLoss.biomassLoss += loss.biomassLoss
          otherLoss.carbonEmissions += loss.carbonEmissions
          otherLoss.grossEmissionsCo2eCo2Only += loss.grossEmissionsCo2eCo2Only
          otherLoss.grossEmissionsCo2eNonCo2 += loss.grossEmissionsCo2eNonCo2
          otherLoss.grossEmissionsCo2eAllGases += loss.grossEmissionsCo2eAllGases
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
      ((avgBiomass * treecoverExtent2000) + (other.avgBiomass * other.treecoverExtent2000)) / (treecoverExtent2000 + other.treecoverExtent2000),
      totalGrossCumulAbovegroundRemovalsCo2 + other.totalGrossCumulAbovegroundRemovalsCo2,
      totalGrossCumulBelowgroundRemovalsCo2 + other.totalGrossCumulBelowgroundRemovalsCo2,
      totalGrossCumulAboveBelowgroundRemovalsCo2 + other.totalGrossCumulAboveBelowgroundRemovalsCo2,
      totalGrossEmissionsCo2eCo2Only + other.totalGrossEmissionsCo2eCo2Only,
      totalGrossEmissionsCo2eNonCo2 + other.totalGrossEmissionsCo2eNonCo2,
      totalGrossEmissionsCo2eAllGases + other.totalGrossEmissionsCo2eAllGases,
      totalNetFluxCo2 + other.totalNetFluxCo2,
      totalFluxModelExtentArea + other.totalFluxModelExtentArea
    )
  }
}

object TreeLossData {
  implicit val lossDataSemigroup: Semigroup[TreeLossData] =
    new Semigroup[TreeLossData] {
      def combine(x: TreeLossData, y: TreeLossData): TreeLossData = x.merge(y)
    }

}
