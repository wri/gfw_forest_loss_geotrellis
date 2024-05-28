package org.globalforestwatch.summarystats.afi

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AFiData(
                    var natural_forest__extent: Double,
                    var jrc_forest_cover__extent: Double,
                    var negligible_risk_area__ha: Double,
                    var protected_areas_area__ha: Double,
                    var landmark_area__ha: Double,
                    var total_area__ha: Double
                  ) {
  def merge(other: AFiData): AFiData = {
    AFiData(
      natural_forest__extent + other.natural_forest__extent,
      jrc_forest_cover__extent + other.jrc_forest_cover__extent,
      negligible_risk_area__ha + other.negligible_risk_area__ha,
      protected_areas_area__ha + other.protected_areas_area__ha,
      landmark_area__ha + other.landmark_area__ha,
      total_area__ha + other.total_area__ha
    )
  }
}

object AFiData {
  def empty: AFiData =
    AFiData(0, 0, 0, 0, 0, 0)

  implicit val afiDataSemigroup: Semigroup[AFiData] =
    new Semigroup[AFiData] {
      def combine(x: AFiData, y: AFiData): AFiData = x.merge(y)
    }

}
