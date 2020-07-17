package org.globalforestwatch.summarystats.treecoverloss

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

object TreeLossDF {

  val treecoverLossMinYear = 2001
  val treecoverLossMaxYear = 2019

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data"))

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("treecoverLoss") as s"umd_tree_cover_loss_${i}__ha"
      }).toList
    val primaryLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("primaryLoss") as s"umd_primary_forest_loss_${i}__ha"
      }).toList
    val iflLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("iflLoss") as s"intact_forest_landscapes_2016_loss_${i}__ha"
      }).toList
    val peatlandsLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("peatlandsLoss") as s"peatlands_loss_${i}__ha"
      }).toList
    val protectedAreasLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("wdpaLoss") as s"protected_areas_loss_${i}__ha"
      }).toList

    val cols = List(
      $"id.featureId" as "feature__id",
      $"data.totalArea" as "area__ha",
      $"data.treecoverExtent2000" as "umd_tree_cover_extent_2000__ha",
      $"data.totalLossArea" as "total_umd_tree_cover_loss__ha",
      $"data.primaryForestExtent" as "primary_forest_extent__ha",
      $"data.iflExtent" as "intact_forest_landscapes_2016_extent__ha",
      $"data.peatlandsExtent" as "peatlands_extent__ha",
      $"data.wdpaExtent" as "protected_areas_extent__ha",
      $"data.totalPrimaryForestLoss" as "total_primary_forest_loss__ha",
      $"data.totalIflLoss" as "total_intact_forest_landscapes_2016_loss__ha",
      $"data.totalPeatlandsLoss" as "total_peatlands_loss__ha",
      $"data.totalWdpaLoss" as "total_protected_areas_loss__ha"
    )

    df.select(
      cols ::: treecoverLossCols ::: primaryLossCols ::: iflLossCols ::: peatlandsLossCols ::: protectedAreasLossCols: _*
    )

  }
  /*
  def primaryForestFilter(include: Boolean)(df: DataFrame): DataFrame = {

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

    if (include) df
    else {
      df.groupBy(
          $"feature__id",
        $"umd_tree_cover_density__threshold",
        $"umd_tree_cover_extent__year"
        )
        .agg(
          cols.head,
          cols.tail ::: treecoverLossCols ::: abovegroundBiomassLossCols ::: co2EmissionsCols: _*
        )
    }
  }*/

}
