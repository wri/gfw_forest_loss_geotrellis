package org.globalforestwatch.gladalerts

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  * @param totalAlerts
  * @param alertArea
  * @param co2Emissions
  * @param totalArea
  *
  */
case class GladAlertsData(var totalAlerts: Int,
                          var alertArea: Double,
                          var co2Emissions: Double,
                          var totalArea: Double) {
  def merge(other: GladAlertsData): GladAlertsData = {

    GladAlertsData(
      totalAlerts + other.totalAlerts,
      alertArea + other.alertArea,
      co2Emissions + other.co2Emissions,
      totalArea + other.totalArea
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
