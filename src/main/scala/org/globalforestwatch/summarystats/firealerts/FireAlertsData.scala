package org.globalforestwatch.summarystats.firealerts

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  * @param totalAlerts
  *
  */
case class FireAlertsData(var totalAlerts: Int) {
  def merge(other: FireAlertsData): FireAlertsData = {
    FireAlertsData(
      totalAlerts + other.totalAlerts
    )
  }
}

object FireAlertsData {
  implicit val fireAlertDataSemigroup: Semigroup[FireAlertsData] =
    new Semigroup[FireAlertsData] {
      def combine(x: FireAlertsData, y: FireAlertsData): FireAlertsData =
        x.merge(y)
    }
}
