package org.globalforestwatch.summarystats.firealerts

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  * @param totalAlerts
  *
  */
case class FireAlertsData(var total: Double) {
  def merge(other: FireAlertsData): FireAlertsData = {
    FireAlertsData(
      total + other.total
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
