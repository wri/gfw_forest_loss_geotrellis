package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardData(
  /** Location intersects Integrated Alert tiles, integrated alerts are possible */
  integrated_alerts_coverage: Boolean,
  /** How many hectares of location geometry had tree cover extent > 30%  in 2000 */
  tree_cover_extent_total: ForestChangeDiagnosticDataDouble,
  /** Integrated alert count within location geometry grouped by day */
  integrated_alerts_daily: GfwProDashboardDataDateCount,
  /** Integrated alert count within location geometry grouped by ISO year-week */
  integrated_alerts_weekly: GfwProDashboardDataDateCount,
  /** Integrated alert count within location geometry grouped by year-month */
  integrated_alerts_monthly: GfwProDashboardDataDateCount,
  /** VIIRS alerts for location geometry grouped by day */
  viirs_alerts_daily: GfwProDashboardDataDateCount,
) {

  def merge(other: GfwProDashboardData): GfwProDashboardData = {
    GfwProDashboardData(
      integrated_alerts_coverage || other.integrated_alerts_coverage,
      tree_cover_extent_total.merge(other.tree_cover_extent_total),
      integrated_alerts_daily.merge(other.integrated_alerts_daily),
      integrated_alerts_weekly.merge(other.integrated_alerts_weekly),
      integrated_alerts_monthly.merge(other.integrated_alerts_monthly),
      viirs_alerts_daily.merge(other.viirs_alerts_daily)
    )
  }
}

object GfwProDashboardData {

  def empty: GfwProDashboardData =
    GfwProDashboardData(
      integrated_alerts_coverage = false,
      tree_cover_extent_total = ForestChangeDiagnosticDataDouble.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty
    )

  implicit val gfwProDashboardDataSemigroup: Semigroup[GfwProDashboardData] =
    new Semigroup[GfwProDashboardData] {
      def combine(x: GfwProDashboardData, y: GfwProDashboardData): GfwProDashboardData =
        x.merge(y)
    }

  implicit def dataExpressionEncoder: ExpressionEncoder[GfwProDashboardData] =
    frameless.TypedExpressionEncoder[GfwProDashboardData]
      .asInstanceOf[ExpressionEncoder[GfwProDashboardData]]
}
