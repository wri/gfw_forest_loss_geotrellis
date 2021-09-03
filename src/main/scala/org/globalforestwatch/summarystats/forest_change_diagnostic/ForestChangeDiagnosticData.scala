package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.Semigroup

import scala.collection.immutable.SortedMap
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Summary per class
  *
  * Note: This case class contains mutable values
  */
case class ForestChangeDiagnosticData(
  /** Tree Cover Loss TCD 30 */
  tree_cover_loss_total_yearly: ForestChangeDiagnosticDataLossYearly,
  tree_cover_loss_tcd90_yearly: ForestChangeDiagnosticDataLossYearly,
  tree_cover_loss_primary_forest_yearly: ForestChangeDiagnosticDataLossYearly,
  tree_cover_loss_peat_yearly: ForestChangeDiagnosticDataLossYearly,
  tree_cover_loss_intact_forest_yearly: ForestChangeDiagnosticDataLossYearly,
  tree_cover_loss_protected_areas_yearly: ForestChangeDiagnosticDataLossYearly,
  /** Tree cover loss in south east asia */
  tree_cover_loss_sea_landcover_yearly: ForestChangeDiagnosticDataLossYearlyCategory,
  tree_cover_loss_idn_landcover_yearly: ForestChangeDiagnosticDataLossYearlyCategory,
  /** treeCoverLossSoyPlanedAreasYearly */
  tree_cover_loss_soy_yearly: ForestChangeDiagnosticDataLossYearly,
  /** treeCoverLossIDNForestAreaYearly */
  tree_cover_loss_idn_legal_yearly: ForestChangeDiagnosticDataLossYearlyCategory,
  tree_cover_loss_idn_forest_moratorium_yearly: ForestChangeDiagnosticDataLossYearly,
  tree_cover_loss_prodes_yearly: ForestChangeDiagnosticDataLossYearly,
  /** prodesLossProtectedAreasYearly */
  tree_cover_loss_prodes_wdpa_yearly: ForestChangeDiagnosticDataLossYearly,
  tree_cover_loss_prodes_primary_forest_yearly: ForestChangeDiagnosticDataLossYearly,
  tree_cover_loss_brazil_biomes_yearly: ForestChangeDiagnosticDataLossYearlyCategory,
  tree_cover_extent_total: ForestChangeDiagnosticDataDouble,
  tree_cover_extent_primary_forest: ForestChangeDiagnosticDataDouble,
  tree_cover_extent_protected_areas: ForestChangeDiagnosticDataDouble,
  tree_cover_extent_peat: ForestChangeDiagnosticDataDouble,
  tree_cover_extent_intact_forest: ForestChangeDiagnosticDataDouble,
  /** Primary Forest Area */
  natural_habitat_primary: ForestChangeDiagnosticDataDouble,
  /** Intact Forest 2016 Area */
  natural_habitat_intact_forest: ForestChangeDiagnosticDataDouble,
  total_area: ForestChangeDiagnosticDataDouble,
  protected_areas_area: ForestChangeDiagnosticDataDouble,
  /** Peatland Area */
  peat_area: ForestChangeDiagnosticDataDouble,
  brazil_biomes: ForestChangeDiagnosticDataDoubleCategory,
  /** IDN Forest Area */
  idn_legal_area: ForestChangeDiagnosticDataDoubleCategory,
  /** Southeast Asia land cover area */
  sea_landcover_area: ForestChangeDiagnosticDataDoubleCategory,
  idn_landcover_area: ForestChangeDiagnosticDataDoubleCategory,
  idn_forest_moratorium_area: ForestChangeDiagnosticDataDouble,
  south_america_presence: ForestChangeDiagnosticDataBoolean,
  legal_amazon_presence: ForestChangeDiagnosticDataBoolean,
  brazil_biomes_presence: ForestChangeDiagnosticDataBoolean,
  cerrado_biome_presence: ForestChangeDiagnosticDataBoolean,
  southeast_asia_presence: ForestChangeDiagnosticDataBoolean,
  indonesia_presence: ForestChangeDiagnosticDataBoolean,
  filtered_tree_cover_extent: ForestChangeDiagnosticDataDouble,
  filtered_tree_cover_extent_yearly: ForestChangeDiagnosticDataValueYearly,
  filtered_tree_cover_loss_yearly: ForestChangeDiagnosticDataLossYearly,
  filtered_tree_cover_loss_peat_yearly: ForestChangeDiagnosticDataLossYearly,
  filtered_tree_cover_loss_protected_areas_yearly: ForestChangeDiagnosticDataLossYearly,
  plantation_area: ForestChangeDiagnosticDataDouble,
  plantation_on_peat_area: ForestChangeDiagnosticDataDouble,
  plantation_in_protected_areas_area: ForestChangeDiagnosticDataDouble,
  commodity_value_forest_extent: ForestChangeDiagnosticDataValueYearly,
  commodity_value_peat: ForestChangeDiagnosticDataValueYearly,
  commodity_value_protected_areas: ForestChangeDiagnosticDataValueYearly,
  commodity_threat_deforestation: ForestChangeDiagnosticDataLossYearly,
  commodity_threat_peat: ForestChangeDiagnosticDataLossYearly,
  commodity_threat_protected_areas: ForestChangeDiagnosticDataLossYearly,
  commodity_threat_fires: ForestChangeDiagnosticDataLossYearly
) {

  def merge(other: ForestChangeDiagnosticData): ForestChangeDiagnosticData = {

    ForestChangeDiagnosticData(
      tree_cover_loss_total_yearly.merge(other.tree_cover_loss_total_yearly),
      tree_cover_loss_tcd90_yearly.merge(other.tree_cover_loss_tcd90_yearly),
      tree_cover_loss_primary_forest_yearly.merge(
        other.tree_cover_loss_primary_forest_yearly
      ),
      tree_cover_loss_peat_yearly.merge(other.tree_cover_loss_peat_yearly),
      tree_cover_loss_intact_forest_yearly.merge(
        other.tree_cover_loss_intact_forest_yearly
      ),
      tree_cover_loss_protected_areas_yearly.merge(
        other.tree_cover_loss_protected_areas_yearly
      ),
      tree_cover_loss_sea_landcover_yearly.merge(
        other.tree_cover_loss_sea_landcover_yearly
      ),
      tree_cover_loss_idn_landcover_yearly.merge(
        other.tree_cover_loss_idn_landcover_yearly
      ),
      tree_cover_loss_soy_yearly.merge(
        other.tree_cover_loss_soy_yearly
      ),
      tree_cover_loss_idn_legal_yearly.merge(
        other.tree_cover_loss_idn_legal_yearly
      ),
      tree_cover_loss_idn_forest_moratorium_yearly.merge(
        other.tree_cover_loss_idn_forest_moratorium_yearly
      ),
      tree_cover_loss_prodes_yearly.merge(other.tree_cover_loss_prodes_yearly),
      tree_cover_loss_prodes_wdpa_yearly.merge(
        other.tree_cover_loss_prodes_wdpa_yearly
      ),
      tree_cover_loss_prodes_primary_forest_yearly.merge(
        other.tree_cover_loss_prodes_primary_forest_yearly
      ),
      tree_cover_loss_brazil_biomes_yearly.merge(other.tree_cover_loss_brazil_biomes_yearly),
      tree_cover_extent_total.merge(other.tree_cover_extent_total),
      tree_cover_extent_primary_forest.merge(other.tree_cover_extent_primary_forest),
      tree_cover_extent_protected_areas.merge(other.tree_cover_extent_protected_areas),
      tree_cover_extent_peat.merge(other.tree_cover_extent_peat),
      tree_cover_extent_intact_forest.merge(other.tree_cover_extent_intact_forest),
      natural_habitat_primary.merge(other.natural_habitat_primary),
      natural_habitat_intact_forest.merge(other.natural_habitat_intact_forest),
      total_area.merge(other.total_area),
      protected_areas_area.merge(other.protected_areas_area),
      peat_area.merge(other.peat_area),
      brazil_biomes.merge(other.brazil_biomes),
      idn_legal_area.merge(other.idn_legal_area),
      sea_landcover_area.merge(other.sea_landcover_area),
      idn_landcover_area.merge(other.idn_landcover_area),
      idn_forest_moratorium_area.merge(other.idn_forest_moratorium_area),
      south_america_presence.merge(other.south_america_presence),
      legal_amazon_presence.merge(other.legal_amazon_presence),
      brazil_biomes_presence.merge(other.brazil_biomes_presence),
      cerrado_biome_presence.merge(other.cerrado_biome_presence),
      southeast_asia_presence.merge(other.southeast_asia_presence),
      indonesia_presence.merge(other.indonesia_presence),
      filtered_tree_cover_extent.merge(other.filtered_tree_cover_extent),
      filtered_tree_cover_extent_yearly.merge(other.filtered_tree_cover_extent_yearly),
      filtered_tree_cover_loss_yearly.merge(other.filtered_tree_cover_loss_yearly),
      filtered_tree_cover_loss_peat_yearly.merge(
        other.filtered_tree_cover_loss_peat_yearly
      ),
      filtered_tree_cover_loss_protected_areas_yearly.merge(
        other.filtered_tree_cover_loss_protected_areas_yearly
      ),
      plantation_area.merge(other.plantation_area),
      plantation_on_peat_area.merge(other.plantation_on_peat_area),
      plantation_in_protected_areas_area.merge(
        other.plantation_in_protected_areas_area
      ),
      commodity_value_forest_extent.merge(other.commodity_value_forest_extent),
      commodity_value_peat.merge(other.commodity_value_peat),
      commodity_value_protected_areas.merge(other.commodity_value_protected_areas),
      commodity_threat_deforestation.merge(other.commodity_threat_deforestation),
      commodity_threat_peat.merge(other.commodity_threat_peat),
      commodity_threat_protected_areas.merge(other.commodity_threat_protected_areas),
      commodity_threat_fires.merge(other.commodity_threat_fires)
    )
  }

  def withUpdatedCommodityRisk(): ForestChangeDiagnosticData = {
    val minLossYear =
      ForestChangeDiagnosticDataLossYearly.prefilled.value.keysIterator.min

    val maxLossYear =
      ForestChangeDiagnosticDataLossYearly.prefilled.value.keysIterator.max

    val years: List[Int] = List.range(minLossYear + 1, maxLossYear + 1)

    val forestValueIndicator: ForestChangeDiagnosticDataValueYearly =
      ForestChangeDiagnosticDataValueYearly.fill(
        filtered_tree_cover_extent.value,
        filtered_tree_cover_loss_yearly.value,
        2
      )
    val peatValueIndicator: ForestChangeDiagnosticDataValueYearly =
      ForestChangeDiagnosticDataValueYearly.fill(peat_area.value)
    val protectedAreaValueIndicator: ForestChangeDiagnosticDataValueYearly =
      ForestChangeDiagnosticDataValueYearly.fill(protected_areas_area.value)
    val deforestationThreatIndicator: ForestChangeDiagnosticDataLossYearly =
      ForestChangeDiagnosticDataLossYearly(
        SortedMap(
          years.map(
            year =>
              (year, {
                // Somehow the compiler cannot infer the types correctly
                // I hence declare them here explicitly to help him out.
                val thisYearLoss: Double =
                filtered_tree_cover_loss_yearly.value
                  .getOrElse(year, 0)

                val lastYearLoss: Double =
                  filtered_tree_cover_loss_yearly.value
                    .getOrElse(year - 1, 0)

                thisYearLoss + lastYearLoss
              })
          ): _*
        )
      )
    val peatThreatIndicator: ForestChangeDiagnosticDataLossYearly =
      ForestChangeDiagnosticDataLossYearly(
        SortedMap(
          years.map(
            year =>
              (year, {
                // Somehow the compiler cannot infer the types correctly
                // I hence declare them here explicitly to help him out.
                val thisYearPeatLoss: Double =
                filtered_tree_cover_loss_peat_yearly.value
                  .getOrElse(year, 0)

                val lastYearPeatLoss: Double =
                  filtered_tree_cover_loss_peat_yearly.value
                    .getOrElse(year - 1, 0)

                thisYearPeatLoss + lastYearPeatLoss + plantation_on_peat_area.value

              })
          ): _*
        )
      )
    val protectedAreaThreatIndicator: ForestChangeDiagnosticDataLossYearly =
      ForestChangeDiagnosticDataLossYearly(
        SortedMap(
          years.map(
            year =>
              (year, {
                // Somehow the compiler cannot infer the types correctly
                // I hence declare them here explicitly to help him out.
                val thisYearProtectedAreaLoss: Double =
                filtered_tree_cover_loss_protected_areas_yearly.value
                  .getOrElse(year, 0)

                val lastYearProtectedAreaLoss: Double =
                  filtered_tree_cover_loss_protected_areas_yearly.value
                    .getOrElse(year - 1, 0)

                thisYearProtectedAreaLoss + lastYearProtectedAreaLoss + plantation_in_protected_areas_area.value
              })
          ): _*
        )
      )

    copy(
      commodity_value_forest_extent = forestValueIndicator,
      commodity_value_peat = peatValueIndicator,
      commodity_value_protected_areas = protectedAreaValueIndicator,
      commodity_threat_deforestation = deforestationThreatIndicator,
      commodity_threat_peat = peatThreatIndicator,
      commodity_threat_protected_areas = protectedAreaThreatIndicator)
  }

}

object ForestChangeDiagnosticData {

  def empty: ForestChangeDiagnosticData =
    ForestChangeDiagnosticData(
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearlyCategory.empty,
      ForestChangeDiagnosticDataLossYearlyCategory.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearlyCategory.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearlyCategory.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDoubleCategory.empty,
      ForestChangeDiagnosticDataDoubleCategory.empty,
      ForestChangeDiagnosticDataDoubleCategory.empty,
      ForestChangeDiagnosticDataDoubleCategory.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataValueYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataValueYearly.empty,
      ForestChangeDiagnosticDataValueYearly.empty,
      ForestChangeDiagnosticDataValueYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
    )

  implicit val lossDataSemigroup: Semigroup[ForestChangeDiagnosticData] =
    new Semigroup[ForestChangeDiagnosticData] {
      def combine(x: ForestChangeDiagnosticData,
                  y: ForestChangeDiagnosticData): ForestChangeDiagnosticData =
        x.merge(y)
    }

  implicit def dataExpressionEncoder: ExpressionEncoder[ForestChangeDiagnosticData] =
    frameless.TypedExpressionEncoder[ForestChangeDiagnosticData]
      .asInstanceOf[ExpressionEncoder[ForestChangeDiagnosticData]]
}
