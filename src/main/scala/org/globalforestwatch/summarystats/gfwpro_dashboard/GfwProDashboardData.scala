package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.Semigroup
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class GfwProDashboardData(
  // Relevant for dissolved locations (locationId == -1)
  group_gadm_id: String,

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
    * x alert confidence (nominal, high, highest) */
  integrated_alerts_daily_all_nominal: GfwProDashboardDataDateCount,
  integrated_alerts_daily_all_high: GfwProDashboardDataDateCount,
  integrated_alerts_daily_all_highest: GfwProDashboardDataDateCount,
  integrated_alerts_daily_sbtn_nominal: GfwProDashboardDataDateCount,
  integrated_alerts_daily_sbtn_high: GfwProDashboardDataDateCount,
  integrated_alerts_daily_sbtn_highest: GfwProDashboardDataDateCount,
  integrated_alerts_daily_jrc_nominal: GfwProDashboardDataDateCount,
  integrated_alerts_daily_jrc_high: GfwProDashboardDataDateCount,
  integrated_alerts_daily_jrc_highest: GfwProDashboardDataDateCount,

  /** Area of Integrated dist alerts within location geometry grouped by day, and further subdivided by cross-product of cover area (all, SBTN natural forest, SBTN natural open ecosystem, JRC area) x alert confidence (nominal, high, highest) */
  intdist_alerts_area_all_nominal: GfwProDashboardDataDateArea,
  intdist_alerts_area_all_high: GfwProDashboardDataDateArea,
  intdist_alerts_area_all_highest: GfwProDashboardDataDateArea,
  intdist_alerts_area_sbtn_natural_forests_nominal: GfwProDashboardDataDateArea,
  intdist_alerts_area_sbtn_natural_forests_high: GfwProDashboardDataDateArea,
  intdist_alerts_area_sbtn_natural_forests_highest: GfwProDashboardDataDateArea,
  intdist_alerts_area_sbtn_natural_ecosystem_nominal: GfwProDashboardDataDateArea,
  intdist_alerts_area_sbtn_natural_ecosystem_high: GfwProDashboardDataDateArea,
  intdist_alerts_area_sbtn_natural_ecosystem_highest: GfwProDashboardDataDateArea,
  intdist_alerts_area_jrc_nominal: GfwProDashboardDataDateArea,
  intdist_alerts_area_jrc_high: GfwProDashboardDataDateArea,
  intdist_alerts_area_jrc_highest: GfwProDashboardDataDateArea,

  /** Integrated alert count within location geometry grouped by ISO year-week */
  glad_alerts_weekly: GfwProDashboardDataDateCount,
  /** Integrated alert count within location geometry grouped by year-month */
  glad_alerts_monthly: GfwProDashboardDataDateCount,
  /** VIIRS alerts for location geometry grouped by day */
  viirs_alerts_daily: GfwProDashboardDataDateCount,
) {

  def merge(other: GfwProDashboardData): GfwProDashboardData = {
    GfwProDashboardData(
      if (group_gadm_id != "") group_gadm_id else other.group_gadm_id,
      glad_alerts_coverage || other.glad_alerts_coverage,
      integrated_alerts_coverage || other.integrated_alerts_coverage,
      total_ha.merge(other.total_ha),
      tree_cover_extent_total.merge(other.tree_cover_extent_total),
      glad_alerts_daily.merge(other.glad_alerts_daily),

      integrated_alerts_daily_all_nominal.merge(other.integrated_alerts_daily_all_nominal),
      integrated_alerts_daily_all_high.merge(other.integrated_alerts_daily_all_high),
      integrated_alerts_daily_all_highest.merge(other.integrated_alerts_daily_all_highest),
      integrated_alerts_daily_sbtn_nominal.merge(other.integrated_alerts_daily_sbtn_nominal),
      integrated_alerts_daily_sbtn_high.merge(other.integrated_alerts_daily_sbtn_high),
      integrated_alerts_daily_sbtn_highest.merge(other.integrated_alerts_daily_sbtn_highest),
      integrated_alerts_daily_jrc_nominal.merge(other.integrated_alerts_daily_jrc_nominal),
      integrated_alerts_daily_jrc_high.merge(other.integrated_alerts_daily_jrc_high),
      integrated_alerts_daily_jrc_highest.merge(other.integrated_alerts_daily_jrc_highest),

      intdist_alerts_area_all_nominal.merge(other.intdist_alerts_area_all_nominal),
      intdist_alerts_area_all_high.merge(other.intdist_alerts_area_all_high),
      intdist_alerts_area_all_highest.merge(other.intdist_alerts_area_all_highest),
      intdist_alerts_area_sbtn_natural_forests_nominal.merge(other.intdist_alerts_area_sbtn_natural_forests_nominal),
      intdist_alerts_area_sbtn_natural_forests_high.merge(other.intdist_alerts_area_sbtn_natural_forests_high),
      intdist_alerts_area_sbtn_natural_forests_highest.merge(other.intdist_alerts_area_sbtn_natural_forests_highest),
      intdist_alerts_area_sbtn_natural_ecosystem_nominal.merge(other.intdist_alerts_area_sbtn_natural_ecosystem_nominal),
      intdist_alerts_area_sbtn_natural_ecosystem_high.merge(other.intdist_alerts_area_sbtn_natural_ecosystem_high),
      intdist_alerts_area_sbtn_natural_ecosystem_highest.merge(other.intdist_alerts_area_sbtn_natural_ecosystem_highest),
      intdist_alerts_area_jrc_nominal.merge(other.intdist_alerts_area_jrc_nominal),
      intdist_alerts_area_jrc_high.merge(other.intdist_alerts_area_jrc_high),
      intdist_alerts_area_jrc_highest.merge(other.intdist_alerts_area_jrc_highest),

      glad_alerts_weekly.merge(other.glad_alerts_weekly),
      glad_alerts_monthly.merge(other.glad_alerts_monthly),
      viirs_alerts_daily.merge(other.viirs_alerts_daily)
    )
  }
}

object GfwProDashboardData {

  def empty: GfwProDashboardData =
    GfwProDashboardData(
      group_gadm_id = "",
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

      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,
      GfwProDashboardDataDateArea.empty,

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
