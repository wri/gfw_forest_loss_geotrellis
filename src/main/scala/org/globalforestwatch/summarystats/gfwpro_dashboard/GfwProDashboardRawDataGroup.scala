package org.globalforestwatch.summarystats.gfwpro_dashboard

import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble

case class GfwProDashboardRawDataGroup(
  alertCoverage: Boolean,
  alertDate: Option[String],
  isTreeCoverExtent30: Boolean
) {
    def toGfwProDashboardData(alertCount: Int, totalArea: Double) :  GfwProDashboardData = {
      GfwProDashboardData(
        alertCoverage,
        glad_alerts_daily = GfwProDashboardDataDateCount.fill(alertDate, alertCount),
        glad_alerts_weekly = GfwProDashboardDataDateCount.fill(alertDate, alertCount, weekly = true),
        glad_alerts_monthly = GfwProDashboardDataDateCount.fill(alertDate, alertCount, monthly = true),
        viirs_alerts_daily = GfwProDashboardDataDateCount.empty,
        tree_cover_extent_total = ForestChangeDiagnosticDataDouble.fill(totalArea, isTreeCoverExtent30))
  }
}
