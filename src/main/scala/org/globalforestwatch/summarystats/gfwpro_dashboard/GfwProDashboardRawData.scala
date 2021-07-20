package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardRawData(
                                   var alertCount: Int,

                                 ) {
  def merge(other: GfwProDashboardRawData): GfwProDashboardRawData = {

    GfwProDashboardRawData(

      alertCount + other.alertCount,

    )
  }
}


object GfwProDashboardRawData {
  implicit val lossDataSemigroup: Semigroup[GfwProDashboardRawData] =
    new Semigroup[GfwProDashboardRawData] {
      def combine(x: GfwProDashboardRawData,
                  y: GfwProDashboardRawData): GfwProDashboardRawData =
        x.merge(y)
    }

}

