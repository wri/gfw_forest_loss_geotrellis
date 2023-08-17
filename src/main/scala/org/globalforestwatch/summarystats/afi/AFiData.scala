package org.globalforestwatch.summarystats.afi

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AFiData(
                    var natural_forest_extent: Double,
                    var natural_forest_loss_area: Double,
                    var tree_cover_loss_area: Double,
                    var negligible_risk_area: Double,
                    var total_area: Double
                  ) {
  def merge(other: AFiData): AFiData = {
    AFiData(
      natural_forest_extent + other.natural_forest_extent,
      natural_forest_loss_area + other.natural_forest_loss_area,
      tree_cover_loss_area + other.tree_cover_loss_area,
      negligible_risk_area + other.negligible_risk_area,
      total_area + other.total_area
    )
  }
}

object AFiData {
  def empty: AFiData =
    AFiData(0, 0, 0, 0, 0)

  implicit val afiDataSemigroup: Semigroup[AFiData] =
    new Semigroup[AFiData] {
      def combine(x: AFiData, y: AFiData): AFiData = x.merge(y)
    }

}
