package org.globalforestwatch.summarystats.afi

import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import java.time.LocalDate

case class AFiRawDataGroup(
  treeCoverLoss: Integer
) {
    def toAFiData(alertCount: Int, totalArea: Double): AFiData = {
      AFiData(
        glad_alerts_coverage = gladAlertsCoverage,
        glad_alerts_daily = AFiDataDateCount.fillDaily(alertDate, alertCount),
        glad_alerts_weekly = AFiDataDateCount.fillWeekly(alertDate, alertCount),
        glad_alerts_monthly = AFiDataDateCount.fillMonthly(alertDate, alertCount),
        viirs_alerts_daily = AFiDataDateCount.empty,
        tree_cover_extent_total = ForestChangeDiagnosticDataDouble.fill(totalArea))
  }
}
