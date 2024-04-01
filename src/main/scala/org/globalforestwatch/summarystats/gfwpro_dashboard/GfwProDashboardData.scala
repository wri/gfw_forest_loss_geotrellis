package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardData(
  /* NOTE: We are temporarily leaving the integrated_alerts_* fields named as
   * glad_alerts_*, in order to reduce the number of moving pieces as we move from
   * Glad alerts to integrated alerts in GFWPro. */

  /** Location intersects Integrated Alert tiles, integrated alerts are possible */
  glad_alerts_coverage: Boolean,
  /** Total hectares in location geometry */
  total_ha: ForestChangeDiagnosticDataDouble,
  /** How many hectares of location geometry had tree cover extent > 30%  in 2000 */
  tree_cover_extent_total: ForestChangeDiagnosticDataDouble,
  /** Integrated alert count within location geometry grouped by day, over the whole
    * area and including all confidences */
  glad_alerts_daily: GfwProDashboardDataDateCount,
  /** Integrated alert count within location geometry grouped by day, and further
    * subdivided by cross-product of cover area (all, SBTN area only, JRC area only)
    * x alert confidence (lo, med, hi) */
  glad_alerts_daily_all_lo: GfwProDashboardDataDateCount,
  glad_alerts_daily_all_med: GfwProDashboardDataDateCount,
  glad_alerts_daily_all_hi: GfwProDashboardDataDateCount,
  glad_alerts_daily_sbtn_lo: GfwProDashboardDataDateCount,
  glad_alerts_daily_sbtn_med: GfwProDashboardDataDateCount,
  glad_alerts_daily_sbtn_hi: GfwProDashboardDataDateCount,
  glad_alerts_daily_jrc_lo: GfwProDashboardDataDateCount,
  glad_alerts_daily_jrc_med: GfwProDashboardDataDateCount,
  glad_alerts_daily_jrc_hi: GfwProDashboardDataDateCount,
  /** Integrated alert count within location geometry grouped by ISO year-week */
  glad_alerts_weekly: GfwProDashboardDataDateCount,
  /** Integrated alert count within location geometry grouped by year-month */
  glad_alerts_monthly: GfwProDashboardDataDateCount,
  /** VIIRS alerts for location geometry grouped by day */
  viirs_alerts_daily: GfwProDashboardDataDateCount,
) {

  def merge(other: GfwProDashboardData): GfwProDashboardData = {
    GfwProDashboardData(
      glad_alerts_coverage || other.glad_alerts_coverage,
      total_ha.merge(other.total_ha),
      tree_cover_extent_total.merge(other.tree_cover_extent_total),
      glad_alerts_daily.merge(other.glad_alerts_daily),
      glad_alerts_daily_all_lo.merge(other.glad_alerts_daily_all_lo),
      glad_alerts_daily_all_med.merge(other.glad_alerts_daily_all_med),
      glad_alerts_daily_all_hi.merge(other.glad_alerts_daily_all_hi),
      glad_alerts_daily_sbtn_lo.merge(other.glad_alerts_daily_sbtn_lo),
      glad_alerts_daily_sbtn_med.merge(other.glad_alerts_daily_sbtn_med),
      glad_alerts_daily_sbtn_hi.merge(other.glad_alerts_daily_sbtn_hi),
      glad_alerts_daily_jrc_lo.merge(other.glad_alerts_daily_jrc_lo),
      glad_alerts_daily_jrc_med.merge(other.glad_alerts_daily_jrc_med),
      glad_alerts_daily_jrc_hi.merge(other.glad_alerts_daily_jrc_hi),
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
      total_ha = ForestChangeDiagnosticDataDouble.empty,
      tree_cover_extent_total = ForestChangeDiagnosticDataDouble.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
      GfwProDashboardDataDateCount.empty,
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
