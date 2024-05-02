package org.globalforestwatch.summarystats.gfwpro_dashboard

import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import org.globalforestwatch.layers.DateConfLevelsLayer
import java.time.LocalDate


case class GfwProDashboardRawDataGroup(
  alertDateAndConf: Option[(LocalDate, Int)],
  integratedAlertsCoverage: Boolean,
  isNaturalForest: Boolean,
  jrcForestCover: Boolean,
  isTreeCoverExtent30: Boolean
) {
    def toGfwProDashboardData(alertCount: Int, totalArea: Double): GfwProDashboardData = {

      val (alertDate, alertConf) = alertDateAndConf match {
        case Some((date, conf)) => (Some(date), conf)
        case _ => (None, DateConfLevelsLayer.ConfLo)
      }

      GfwProDashboardData(
        glad_alerts_coverage = integratedAlertsCoverage,
        integrated_alerts_coverage = integratedAlertsCoverage,
        glad_alerts_daily = GfwProDashboardDataDateCount.fillDaily(alertDate, true, alertCount),
        integrated_alerts_daily_all_lo = GfwProDashboardDataDateCount.fillDaily(alertDate,
          alertConf == DateConfLevelsLayer.ConfLo, alertCount),
        integrated_alerts_daily_all_med = GfwProDashboardDataDateCount.fillDaily(alertDate,
          alertConf == DateConfLevelsLayer.ConfMed, alertCount),
        integrated_alerts_daily_all_hi = GfwProDashboardDataDateCount.fillDaily(alertDate,
          alertConf == DateConfLevelsLayer.ConfHi, alertCount),
        integrated_alerts_daily_sbtn_lo = GfwProDashboardDataDateCount.fillDaily(alertDate,
          isNaturalForest && alertConf == DateConfLevelsLayer.ConfLo, alertCount),
        integrated_alerts_daily_sbtn_med = GfwProDashboardDataDateCount.fillDaily(alertDate,
          isNaturalForest && alertConf == DateConfLevelsLayer.ConfMed, alertCount),
        integrated_alerts_daily_sbtn_hi = GfwProDashboardDataDateCount.fillDaily(alertDate,
          isNaturalForest && alertConf == DateConfLevelsLayer.ConfHi, alertCount),
        integrated_alerts_daily_jrc_lo = GfwProDashboardDataDateCount.fillDaily(alertDate,
          jrcForestCover && alertConf == DateConfLevelsLayer.ConfLo, alertCount),
        integrated_alerts_daily_jrc_med = GfwProDashboardDataDateCount.fillDaily(alertDate,
          jrcForestCover && alertConf == DateConfLevelsLayer.ConfMed, alertCount),
        integrated_alerts_daily_jrc_hi = GfwProDashboardDataDateCount.fillDaily(alertDate,
          jrcForestCover && alertConf == DateConfLevelsLayer.ConfHi, alertCount),
        glad_alerts_weekly = GfwProDashboardDataDateCount.fillWeekly(alertDate, alertCount),
        glad_alerts_monthly = GfwProDashboardDataDateCount.fillMonthly(alertDate, alertCount),
        viirs_alerts_daily = GfwProDashboardDataDateCount.empty,
        tree_cover_extent_total = if (isTreeCoverExtent30) {
          ForestChangeDiagnosticDataDouble.fill(totalArea)
        } else  {
          ForestChangeDiagnosticDataDouble.empty
        },
        total_ha = ForestChangeDiagnosticDataDouble.fill(totalArea)
      )
  }
}
