package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardRawData(
  var totalArea: Double,            // area of the group of pixels
  var alertCount: Int,              // total integrated alert counts for the pixels
  var intDistAlertArea: Double      // total integrated dist alert area for the pixels
) {
  def merge(other: GfwProDashboardRawData): GfwProDashboardRawData = {
    GfwProDashboardRawData(totalArea + other.totalArea, alertCount + other.alertCount, intDistAlertArea + other.intDistAlertArea)
  }
}

object GfwProDashboardRawData {
  implicit val lossDataSemigroup: Semigroup[GfwProDashboardRawData] = Semigroup.instance(_ merge _)
}
