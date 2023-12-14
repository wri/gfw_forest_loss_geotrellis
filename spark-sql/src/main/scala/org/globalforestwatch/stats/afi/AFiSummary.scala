package org.globalforestwatch.stats.afi

import cats.Monoid
import scala.collection.mutable

case class AFiSummary(
  stats: mutable.Map[String, AFiData]
)

object AFiSummary {
  implicit val afiSummaryMonoidInstance: Monoid[AFiSummary] = new Monoid[AFiSummary] {
    def empty: AFiSummary = AFiSummary(mutable.Map.empty)
    def combine(a: AFiSummary, b: AFiSummary): AFiSummary = {
      val contents = a.stats ++ b.stats.map {
        case (k, v) =>
          k -> Monoid[AFiData].combine(v, a.stats.getOrElse(k, Monoid[AFiData].empty))
      }

      AFiSummary(contents)
    }
  }
}

case class AFiData(
  var natural_forest__extent: Double,
  var natural_forest_loss__ha: Double,
  var negligible_risk_area__ha: Double,
  var total_area__ha: Double
) {
  def merge(other: AFiData): AFiData = {
    AFiData(
      natural_forest__extent + other.natural_forest__extent,
      natural_forest_loss__ha + other.natural_forest_loss__ha,
      negligible_risk_area__ha + other.negligible_risk_area__ha,
      total_area__ha + other.total_area__ha
    )
  }
}

object AFiData {
  implicit val afiDataMonoidInstance: Monoid[AFiData] = new Monoid[AFiData] {
    def empty: AFiData = AFiData(0, 0, 0, 0)
    def combine(a: AFiData, b: AFiData): AFiData = a.merge(b)
  }
}
