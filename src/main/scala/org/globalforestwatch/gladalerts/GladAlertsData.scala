package org.globalforestwatch.gladalerts

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  * @param totalAlerts
  * @param totalArea
  * @param totalCo2
  *
  */
case class GladAlertsData(var totalAlerts: Int,
                          var totalArea: Double,
                          var totalCo2: Double) {
  def merge(other: GladAlertsData): GladAlertsData = {

    GladAlertsData(
      totalAlerts + other.totalAlerts,
      totalArea + other.totalArea,
      totalCo2 + other.totalCo2
    )
  }
}

object GladAlertsData {
  implicit val lossDataSemigroup: Semigroup[GladAlertsData] =
    new Semigroup[GladAlertsData] {
      def combine(x: GladAlertsData, y: GladAlertsData): GladAlertsData =
        x.merge(y)
    }

}
