package org.globalforestwatch.treecoverloss_simple_60

import cats.Semigroup
import geotrellis.raster.histogram.StreamingHistogram

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  * @param totalArea
  */
case class TreeLossData(
  var lossYear: scala.collection.mutable.Map[Int, TreeLossYearData],
  var extent2010: Double,
  var totalArea: Double
) {
  def merge(other: TreeLossData): TreeLossData = {

    TreeLossData(lossYear ++ other.lossYear.map {
      case (k, v) => {
        val loss: TreeLossYearData = lossYear(k)
        var otherLoss: TreeLossYearData = v
        otherLoss.area_loss += loss.area_loss
        k -> otherLoss
      }
    }, extent2010 + other.extent2010, totalArea + other.totalArea)
  }
}

object TreeLossData {
  implicit val lossDataSemigroup: Semigroup[TreeLossData] =
    new Semigroup[TreeLossData] {
      def combine(x: TreeLossData, y: TreeLossData): TreeLossData = x.merge(y)
    }

}
