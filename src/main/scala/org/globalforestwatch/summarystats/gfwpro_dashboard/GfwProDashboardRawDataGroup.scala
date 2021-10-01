package org.globalforestwatch.summarystats.gfwpro_dashboard

import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import java.time.LocalDate

case class GfwProDashboardRawDataGroup(
  alertDate: Option[LocalDate]
) {
    def toGfwProDashboardData(alertCount: Int, totalArea: Double): GfwProDashboardData = {
      GfwProDashboardData(
        glad_alerts_coverage = alertDate.isDefined,
        glad_alerts_daily = GfwProDashboardDataDateCount.fillDaily(alertDate, alertCount),
        glad_alerts_weekly = GfwProDashboardDataDateCount.fillWeekly(alertDate, alertCount),
        glad_alerts_monthly = GfwProDashboardDataDateCount.fillMonthly(alertDate, alertCount),
        viirs_alerts_daily = GfwProDashboardDataDateCount.empty,
        tree_cover_extent_total = ForestChangeDiagnosticDataDouble.fill(totalArea))
  }
}
