package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardRawData(var treeCoverExtentArea: Double, var alertCount: Int) {
  def merge(other: GfwProDashboardRawData): GfwProDashboardRawData = {
    GfwProDashboardRawData(treeCoverExtentArea + other.treeCoverExtentArea, alertCount + other.alertCount)
  }
}

object GfwProDashboardRawData {
  implicit val lossDataSemigroup: Semigroup[GfwProDashboardRawData] = Semigroup.instance(_ merge _)
}
