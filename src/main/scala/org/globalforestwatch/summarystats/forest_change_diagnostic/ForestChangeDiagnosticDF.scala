package org.globalforestwatch.summarystats.forest_change_diagnostic

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

object ForestChangeDiagnosticDF {

  val treecoverLossMinYear = 2001
  val treecoverLossMaxYear = 2019

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("treecoverLoss") as s"umd_tree_cover_loss_${i}__ha"
      }).toList
    val abovegroundBiomassLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("biomassLoss") as s"whrc_aboveground_biomass_loss_${i}__Mg"
      }).toList

    val co2EmissionsCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("carbonEmissions") as s"whrc_aboveground_co2_emissions_${i}__Mg"
      }).toList

    val cols = List(
      $"id.featureId" as "feature__id",
      $"data_group.threshold" as "umd_tree_cover_density__threshold",
      $"data_group.tcdYear" as "umd_tree_cover_extent__year",
      $"data_group.isPrimaryForest" as "is__umd_regional_primary_forest_2001",
      $"data_group.isPlantations" as "is__gfw_plantations",
      $"data.treecoverExtent2000" as "umd_tree_cover_extent_2000__ha",
      $"data.treecoverExtent2010" as "umd_tree_cover_extent_2010__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalGainArea" as "umd_tree_cover_gain_2000-2012__ha",
      $"data.totalBiomass" as "whrc_aboveground_biomass_stock_2000__Mg",
      $"data.avgBiomass" as "avg_whrc_aboveground_biomass_stock_2000__Mg_ha-1",
      $"data.totalCo2" as "whrc_aboveground_co2_stock_2000__Mt"
    )

    df.select(
      cols ::: treecoverLossCols ::: abovegroundBiomassLossCols ::: co2EmissionsCols: _*
    )

  }

  def contextualLayerFilter(
    includePrimaryForest: Boolean,
    includePlantations: Boolean
  )(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"umd_tree_cover_loss_${i}__ha") as s"umd_tree_cover_loss_${i}__ha"
      }).toList
    val abovegroundBiomassLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"whrc_aboveground_biomass_loss_${i}__Mg") as s"whrc_aboveground_biomass_loss_${i}__Mg"
      }).toList

    val co2EmissionsCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"whrc_aboveground_co2_emissions_${i}__Mg") as s"whrc_aboveground_co2_emissions_${i}__Mg"
      }).toList

    val cols = List(
      sum("area__ha") as "area__ha",
      sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
      sum("umd_tree_cover_extent_2010__ha") as "umd_tree_cover_extent_2010__ha",
      sum("umd_tree_cover_gain_2000-2012__ha") as "umd_tree_cover_gain_2000-2012__ha",
      sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
      sum(
        $"avg_whrc_aboveground_biomass_stock_2000__Mg_ha-1" * $"umd_tree_cover_extent_2000__ha"
      ) / sum($"umd_tree_cover_extent_2000__ha") as "avg_whrc_aboveground_biomass_stock_2000__Mg_ha-1",
      sum("whrc_aboveground_co2_stock_2000__Mt") as "whrc_aboveground_co2_stock_2000__Mt"
    )

    val groupByCols = List(
      $"feature__id",
      $"umd_tree_cover_density__threshold",
      $"umd_tree_cover_extent__year"
    )

    val pfGroupByCol = {
      if (includePrimaryForest) List($"is__umd_regional_primary_forest_2001")
      else List()
    }

    val plGroupByCol = {
      if (includePlantations) List($"is__gfw_plantations")
      else List()
    }

    df.groupBy(groupByCols ::: pfGroupByCol ::: plGroupByCol: _*)
      .agg(
        cols.head,
        cols.tail ::: treecoverLossCols ::: abovegroundBiomassLossCols ::: co2EmissionsCols: _*
      )

  }

}
