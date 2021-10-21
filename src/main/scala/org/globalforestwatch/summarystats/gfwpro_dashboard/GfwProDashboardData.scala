package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardData(
  /** Location intersects GLAD Alert tiles, GLAD alerts are possible */
  glad_alerts_coverage: Boolean,
  /** How many hacters of location geometry had tree cover extent > 30%  in 2000 */
  tree_cover_extent_total: ForestChangeDiagnosticDataDouble,
  /** GLAD alert count within location geometry grouped by day */
  glad_alerts_daily: GfwProDashboardDataDateCount,
  /** GLAD alert count within location geometry grouped by ISO year-week */
  glad_alerts_weekly: GfwProDashboardDataDateCount,
  /** GLAD alert count within location geometry grouped by year-month */
  glad_alerts_monthly: GfwProDashboardDataDateCount,
  /** VIIRS alerts for location geometry grouped by day */
  viirs_alerts_daily: GfwProDashboardDataDateCount,
) {

  def merge(other: GfwProDashboardData): GfwProDashboardData = {
    GfwProDashboardData(
      glad_alerts_coverage || other.glad_alerts_coverage,
      tree_cover_extent_total.merge(other.tree_cover_extent_total),
      glad_alerts_daily.merge(other.glad_alerts_daily),
      glad_alerts_weekly.merge(other.glad_alerts_weekly),
      glad_alerts_monthly.merge(other.glad_alerts_monthly),
      viirs_alerts_daily.merge(other.viirs_alerts_daily)
    )
  }
}

object GfwProDashboardData {

  def empty: GfwProDashboardData =
    GfwProDashboardData(
      glad_alerts_coverage = false,
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
