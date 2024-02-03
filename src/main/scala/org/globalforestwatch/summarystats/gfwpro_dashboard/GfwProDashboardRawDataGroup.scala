package org.globalforestwatch.summarystats.gfwpro_dashboard

import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import java.time.LocalDate

case class GfwProDashboardRawDataGroup(
  alertDate: Option[LocalDate],
  integratedAlertsCoverage: Boolean
) {
    def toGfwProDashboardData(totalArea: Double, treeCoverExtentArea: Double, alertCount: Int): GfwProDashboardData = {
      GfwProDashboardData(
        glad_alerts_coverage = integratedAlertsCoverage,
        total_area = ForestChangeDiagnosticDataDouble.fill(totalArea),
        tree_cover_extent_total = ForestChangeDiagnosticDataDouble.fill(treeCoverExtentArea),
        glad_alerts_daily = GfwProDashboardDataDateCount.fillDaily(alertDate, alertCount),
        glad_alerts_weekly = GfwProDashboardDataDateCount.fillWeekly(alertDate, alertCount),
        glad_alerts_monthly = GfwProDashboardDataDateCount.fillMonthly(alertDate, alertCount),
        viirs_alerts_daily = GfwProDashboardDataDateCount.empty
      )
  }
}
