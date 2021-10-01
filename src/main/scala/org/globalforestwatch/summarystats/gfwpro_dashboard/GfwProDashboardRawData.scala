package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardRawData(var totalArea: Double, var alertCount: Int) {
  def merge(other: GfwProDashboardRawData): GfwProDashboardRawData = {
    GfwProDashboardRawData(totalArea + other.totalArea, alertCount + other.alertCount)
  }
}

object GfwProDashboardRawData {
  implicit val lossDataSemigroup: Semigroup[GfwProDashboardRawData] = Semigroup.instance(_ merge _)
}
