package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardData(
                                gladAlertsCoverage: Boolean,
                                gladAlertsDaily: GfwProDashboardDataDateCount,
                                gladAlertsWeekly: GfwProDashboardDataDateCount,
                                gladAlertsMonthly: GfwProDashboardDataDateCount,
                                viirsAlertsDaily: GfwProDashboardDataDateCount,


                              ) {

  def merge(other: GfwProDashboardData): GfwProDashboardData = {

    GfwProDashboardData(
      gladAlertsCoverage || other.gladAlertsCoverage,
      gladAlertsDaily.merge(other.gladAlertsDaily),
      gladAlertsWeekly.merge(other.gladAlertsWeekly),
      gladAlertsMonthly.merge(other.gladAlertsMonthly),
      viirsAlertsDaily.merge(
        other.viirsAlertsDaily
      )
    )
  }

  def update(gladAlertsCoverage: Boolean = this.gladAlertsCoverage,
             gladAlertsDaily: GfwProDashboardDataDateCount = this.gladAlertsDaily,
             gladAlertsWeekly: GfwProDashboardDataDateCount = this.gladAlertsWeekly,
             gladAlertsMonthly: GfwProDashboardDataDateCount = this.gladAlertsMonthly,
             viirsAlertsDaily: GfwProDashboardDataDateCount = this.viirsAlertsDaily,
            ): GfwProDashboardData = {

    GfwProDashboardData(
      gladAlertsCoverage,
      gladAlertsDaily,
      gladAlertsWeekly,
      gladAlertsMonthly,
      viirsAlertsDaily
    )
  }

}


object GfwProDashboardData {

  def empty: GfwProDashboardData =
    GfwProDashboardData(
      gladAlertsCoverage = false,
      GfwProDashboardDataDateCount.empty,
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

