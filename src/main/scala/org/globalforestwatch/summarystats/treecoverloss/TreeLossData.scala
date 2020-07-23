package org.globalforestwatch.summarystats.treecoverloss

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class TreeLossData(
                         var lossYear: scala.collection.mutable.Map[Int, TreeLossYearData],
                         var treecoverExtent2000: Double,
                         var totalArea: Double,
                         var totalLossArea: Double,
                         var primaryForestExtent: Double,
                         var iflExtent: Double,
                         var peatlandsExtent: Double,
                         var wdpaExtent: Double,
                         var totalPrimaryForestLoss: Double,
                         var totalIflLoss: Double,
                         var totalPeatlandsLoss: Double,
                         var totalWdpaLoss: Double,
                         var primaryForestTreeCoverExtent: Double,
                         var iflTreeCoverExtent: Double,
                         var peatlandsTreeCoverExtent: Double,
                         var wdpaTreeCoverExtent: Double
                       ) {
  def merge(other: TreeLossData): TreeLossData = {

    TreeLossData(
      lossYear ++ other.lossYear.map {
        case (k, v) => {
          val loss: TreeLossYearData = lossYear(k)
          val otherLoss: TreeLossYearData = v
          otherLoss.treecoverLoss += loss.treecoverLoss
          otherLoss.primaryLoss += loss.primaryLoss
          otherLoss.iflLoss += loss.iflLoss
          otherLoss.peatlandsLoss += loss.peatlandsLoss
          otherLoss.wdpaLoss += loss.wdpaLoss
          k -> otherLoss
        }
      },
      treecoverExtent2000 + other.treecoverExtent2000,
      totalArea + other.totalArea,
      totalLossArea + other.totalLossArea,
      primaryForestExtent + other.primaryForestExtent,
      iflExtent + other.iflExtent,
      peatlandsExtent + other.peatlandsExtent,
      wdpaExtent + other.wdpaExtent,
      totalPrimaryForestLoss + other.totalPrimaryForestLoss,
      totalIflLoss + other.totalIflLoss,
      totalPeatlandsLoss + other.totalPeatlandsLoss,
      totalWdpaLoss + other.totalWdpaLoss,
      iflTreeCoverExtent + other.iflTreeCoverExtent,
      peatlandsTreeCoverExtent + other.peatlandsTreeCoverExtent,
      wdpaTreeCoverExtent + other.wdpaTreeCoverExtent,
      primaryForestExtent + other.primaryForestExtent
    )
  }
}

object TreeLossData {
  implicit val lossDataSemigroup: Semigroup[TreeLossData] =
    new Semigroup[TreeLossData] {
      def combine(x: TreeLossData, y: TreeLossData): TreeLossData = x.merge(y)
    }

}
