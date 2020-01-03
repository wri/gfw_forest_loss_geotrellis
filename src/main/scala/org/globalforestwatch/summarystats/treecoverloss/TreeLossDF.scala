package org.globalforestwatch.summarystats.treecoverloss

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

object TreeLossDF {

  val treecoverLossMinYear = 2001
  val treecoverLossMaxYear = 2018

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("treecoverLoss") as s"treecover_loss_${i}__ha"
      }).toList
    val abovegroundBiomassLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("biomassLoss") as s"aboveground_biomass_loss_${i}__Mg"
      }).toList

    val co2EmissionsCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("carbonEmissions") as s"co2_emissions_${i}__Mg"
      }).toList

    val cols = List(
      $"id.featureId" as "feature__id",
      $"data_group.threshold" as "treecover_density__threshold",
      $"data_group.tcdYear" as "treecover_extent__year",
      $"data_group.primaryForest" as "is__regional_primary_forest",
      $"data.treecoverExtent2000" as "treecover_extent_2000__ha",
      $"data.treecoverExtent2010" as "treecover_extent_2010__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalGainArea" as "treecover_gain_2000-2012__ha",
      $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
      $"data.avgBiomass" as "avg_aboveground_biomass_stock_2000__Mg_ha-1",
      $"data.totalCo2" as "co2_stock_2000__Mt"
    )

    df.select(
      cols ::: treecoverLossCols ::: abovegroundBiomassLossCols ::: co2EmissionsCols: _*
    )

  }

  def primaryForestFilter(include: Boolean)(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"treecover_loss_${i}__ha") as s"treecover_loss_${i}__ha"
      }).toList
    val abovegroundBiomassLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"aboveground_biomass_loss_${i}__Mg") as s"aboveground_biomass_loss_${i}__Mg"
      }).toList

    val co2EmissionsCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"co2_emissions_${i}__Mg") as s"co2_emissions_${i}__Mg"
      }).toList

    val cols = List(
      sum("area__ha") as "area__ha",
      sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
      sum("treecover_extent_2010__ha") as "treecover_extent_2010__ha",
      sum("treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
      sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
      sum(
        $"avg_aboveground_biomass_stock_2000__Mg_ha-1" * $"treecover_extent_2000__ha"
      ) / sum($"treecover_extent_2000__ha") as "avg_aboveground_biomass_stock_2000__Mg_ha-1",
      sum("co2_stock_2000__Mt") as "co2_stock_2000__Mt"
    )

    if (include) df
    else {
      df.groupBy(
          $"feature__id",
          $"treecover_density__threshold",
          $"treecover_extent__year"
        )
        .agg(
          cols.head,
          cols.tail ::: treecoverLossCols ::: abovegroundBiomassLossCols ::: co2EmissionsCols: _*
        )
    }
  }

}
