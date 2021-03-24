package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardData(
                                gladAlertsDaily: GfwProDashboardDataDateCount,
                                gladAlertsMonthly: GfwProDashboardDataDateCount,
                                viirsAlertsDaily: GfwProDashboardDataDateCount,


                              ) {

  def merge(other: GfwProDashboardData): GfwProDashboardData = {

    GfwProDashboardData(
      gladAlertsDaily.merge(other.gladAlertsDaily),
      gladAlertsMonthly.merge(other.gladAlertsMonthly),
      viirsAlertsDaily.merge(
        other.viirsAlertsDaily
      )
    )
  }

  def update(gladAlertsDaily: GfwProDashboardDataDateCount = this.gladAlertsDaily,
             gladAlertsMonthly: GfwProDashboardDataDateCount = this.gladAlertsMonthly,
             viirsAlertsDaily: GfwProDashboardDataDateCount = this.viirsAlertsDaily,
            ): GfwProDashboardData = {

    GfwProDashboardData(
      gladAlertsDaily,
      gladAlertsMonthly,
      viirsAlertsDaily
    )
  }

}


object GfwProDashboardData {

  def empty: GfwProDashboardData =
    GfwProDashboardData(
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,

    )

  implicit val gfwProDashboardDataSemigroup: Semigroup[GfwProDashboardData] =
    new Semigroup[GfwProDashboardData] {
      def combine(x: GfwProDashboardData,
                  y: GfwProDashboardData): GfwProDashboardData =
        x.merge(y)
    }

}

