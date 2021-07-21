package org.globalforestwatch.summarystats.integrated_alerts

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
case class IntegratedAlertsData(var totalAlerts: Int,
                          var alertArea: Double,
                          var co2Emissions: Double,
                          var totalArea: Double) {
  def merge(other: IntegratedAlertsData): IntegratedAlertsData = {

    IntegratedAlertsData(
      totalAlerts + other.totalAlerts,
      alertArea + other.alertArea,
      co2Emissions + other.co2Emissions,
      totalArea + other.totalArea
    )
  }
}

object IntegratedAlertsData {
  implicit val lossDataSemigroup: Semigroup[IntegratedAlertsData] =
    new Semigroup[IntegratedAlertsData] {
      def combine(x: IntegratedAlertsData, y: IntegratedAlertsData): IntegratedAlertsData =
        x.merge(y)
    }

}
