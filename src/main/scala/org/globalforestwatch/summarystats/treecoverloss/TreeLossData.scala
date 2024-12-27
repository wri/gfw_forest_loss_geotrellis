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
                         var avgBiomass: Double,
                         var totalAgc2000: Double,
                         var totalBgc2000: Double,
                         var totalSoilCarbon2000: Double,
                         var totalGrossCumulAbovegroundRemovalsCo2: Double,
                         var totalGrossCumulBelowgroundRemovalsCo2: Double,
                         var totalGrossCumulAboveBelowgroundRemovalsCo2: Double,
                         var totalGrossEmissionsCo2eCo2Only: Double,
                         var totalGrossEmissionsCo2eCh4: Double,
                         var totalGrossEmissionsCo2eN2o: Double,
                         var totalGrossEmissionsCo2eAllGases: Double,
                         var totalNetFluxCo2: Double,
                         var totalFluxModelExtentArea: Double
                       ) {
  def merge(other: TreeLossData): TreeLossData = {
    if (treecoverExtent2000 + other.treecoverExtent2000 > 0) {
      avgBiomass = ((avgBiomass * treecoverExtent2000) + (other.avgBiomass * other.treecoverExtent2000)) / (treecoverExtent2000 + other.treecoverExtent2000)
    } else {
      avgBiomass = 0
    }

    TreeLossData(
      lossYear ++ other.lossYear.map {
        case (k, v) => {
          val loss: TreeLossYearData = lossYear(k)
          val otherLoss: TreeLossYearData = v
          otherLoss.treecoverLoss += loss.treecoverLoss
          otherLoss.biomassLoss += loss.biomassLoss
          otherLoss.grossEmissionsCo2eCo2Only += loss.grossEmissionsCo2eCo2Only
          otherLoss.grossEmissionsCo2eCh4 += loss.grossEmissionsCo2eCh4
          otherLoss.grossEmissionsCo2eN2o += loss.grossEmissionsCo2eN2o
          otherLoss.grossEmissionsCo2eAllGases += loss.grossEmissionsCo2eAllGases
          k -> otherLoss
        }
      },
      treecoverExtent2000 + other.treecoverExtent2000,
      treecoverExtent2010 + other.treecoverExtent2010,
      totalArea + other.totalArea,
      totalGainArea + other.totalGainArea,
      totalBiomass + other.totalBiomass,
      // TODO: use extent2010 to calculate avg biomass incase year is selected
      avgBiomass,
      totalAgc2000 + other.totalAgc2000,
      totalBgc2000 + other.totalBgc2000,
      totalSoilCarbon2000 + other.totalSoilCarbon2000,
      totalGrossCumulAbovegroundRemovalsCo2 + other.totalGrossCumulAbovegroundRemovalsCo2,
      totalGrossCumulBelowgroundRemovalsCo2 + other.totalGrossCumulBelowgroundRemovalsCo2,
      totalGrossCumulAboveBelowgroundRemovalsCo2 + other.totalGrossCumulAboveBelowgroundRemovalsCo2,
      totalGrossEmissionsCo2eCo2Only + other.totalGrossEmissionsCo2eCo2Only,
      totalGrossEmissionsCo2eCh4 + other.totalGrossEmissionsCo2eCh4,
      totalGrossEmissionsCo2eN2o + other.totalGrossEmissionsCo2eN2o,
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
