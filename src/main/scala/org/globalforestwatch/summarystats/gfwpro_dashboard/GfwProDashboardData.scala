package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardData(
  /* NOTE: We are temporarily leaving the existing integrated alerts fields named as
   * glad_alerts_*, in order to reduce the number of moving pieces as we move from
   * Glad alerts to integrated alerts in GFWPro. */

  /** Location intersects Integrated Alert tiles, integrated alerts are possible */
  glad_alerts_coverage: Boolean,
  /** Duplicated column for integrated alerts coverage, to make an easier transition
    * for the GFWPro app. We hope to just remove all the glad_alerts_* columns once
    * the app is only using the integrated_alerts_* columns. */
  integrated_alerts_coverage: Boolean,
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
  integrated_alerts_daily_all_lo: GfwProDashboardDataDateCount,
  integrated_alerts_daily_all_med: GfwProDashboardDataDateCount,
  integrated_alerts_daily_all_hi: GfwProDashboardDataDateCount,
  integrated_alerts_daily_sbtn_lo: GfwProDashboardDataDateCount,
  integrated_alerts_daily_sbtn_med: GfwProDashboardDataDateCount,
  integrated_alerts_daily_sbtn_hi: GfwProDashboardDataDateCount,
  integrated_alerts_daily_jrc_lo: GfwProDashboardDataDateCount,
  integrated_alerts_daily_jrc_med: GfwProDashboardDataDateCount,
  integrated_alerts_daily_jrc_hi: GfwProDashboardDataDateCount,
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
      integrated_alerts_coverage || other.integrated_alerts_coverage,
      total_ha.merge(other.total_ha),
      tree_cover_extent_total.merge(other.tree_cover_extent_total),
      glad_alerts_daily.merge(other.glad_alerts_daily),
      integrated_alerts_daily_all_lo.merge(other.integrated_alerts_daily_all_lo),
      integrated_alerts_daily_all_med.merge(other.integrated_alerts_daily_all_med),
      integrated_alerts_daily_all_hi.merge(other.integrated_alerts_daily_all_hi),
      integrated_alerts_daily_sbtn_lo.merge(other.integrated_alerts_daily_sbtn_lo),
      integrated_alerts_daily_sbtn_med.merge(other.integrated_alerts_daily_sbtn_med),
      integrated_alerts_daily_sbtn_hi.merge(other.integrated_alerts_daily_sbtn_hi),
      integrated_alerts_daily_jrc_lo.merge(other.integrated_alerts_daily_jrc_lo),
      integrated_alerts_daily_jrc_med.merge(other.integrated_alerts_daily_jrc_med),
      integrated_alerts_daily_jrc_hi.merge(other.integrated_alerts_daily_jrc_hi),
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
      integrated_alerts_coverage = false,
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
