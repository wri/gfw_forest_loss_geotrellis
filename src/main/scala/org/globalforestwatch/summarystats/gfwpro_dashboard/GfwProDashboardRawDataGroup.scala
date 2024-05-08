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
        case _ => (None, DateConfLevelsLayer.ConfNominal)
      }

      GfwProDashboardData(
        glad_alerts_coverage = integratedAlertsCoverage,
        integrated_alerts_coverage = integratedAlertsCoverage,
        glad_alerts_daily = GfwProDashboardDataDateCount.fillDaily(alertDate, true, alertCount),
        integrated_alerts_daily_all_nominal = GfwProDashboardDataDateCount.fillDaily(alertDate,
          alertConf == DateConfLevelsLayer.ConfNominal, alertCount),
        integrated_alerts_daily_all_high = GfwProDashboardDataDateCount.fillDaily(alertDate,
          alertConf == DateConfLevelsLayer.ConfHigh, alertCount),
        integrated_alerts_daily_all_highest = GfwProDashboardDataDateCount.fillDaily(alertDate,
          alertConf == DateConfLevelsLayer.ConfHighest, alertCount),
        integrated_alerts_daily_sbtn_nominal = GfwProDashboardDataDateCount.fillDaily(alertDate,
          isNaturalForest && alertConf == DateConfLevelsLayer.ConfNominal, alertCount),
        integrated_alerts_daily_sbtn_high = GfwProDashboardDataDateCount.fillDaily(alertDate,
          isNaturalForest && alertConf == DateConfLevelsLayer.ConfHigh, alertCount),
        integrated_alerts_daily_sbtn_highest = GfwProDashboardDataDateCount.fillDaily(alertDate,
          isNaturalForest && alertConf == DateConfLevelsLayer.ConfHighest, alertCount),
        integrated_alerts_daily_jrc_nominal = GfwProDashboardDataDateCount.fillDaily(alertDate,
          jrcForestCover && alertConf == DateConfLevelsLayer.ConfNominal, alertCount),
        integrated_alerts_daily_jrc_high = GfwProDashboardDataDateCount.fillDaily(alertDate,
          jrcForestCover && alertConf == DateConfLevelsLayer.ConfHigh, alertCount),
        integrated_alerts_daily_jrc_highest = GfwProDashboardDataDateCount.fillDaily(alertDate,
          jrcForestCover && alertConf == DateConfLevelsLayer.ConfHighest, alertCount),
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
