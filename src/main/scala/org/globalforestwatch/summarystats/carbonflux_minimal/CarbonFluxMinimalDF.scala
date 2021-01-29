package org.globalforestwatch.summarystats.carbonflux_minimal

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

object CarbonFluxMinimalDF {

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

//    val GrossEmissionsCo2eCo2OnlyCols =
//      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
//        $"data.lossYear"
//          .getItem(i)
//          .getItem("grossEmissionsCo2eCo2Only") as s"gfw_gross_emissions_co2e_co2_only_${i}__Mg"
//      }).toList
//    val totalGrossEmissionsCo2eNonCo2Cols =
//      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
//        $"data.lossYear"
//          .getItem(i)
//          .getItem("grossEmissionsCo2eNonCo2") as s"gfw_gross_emissions_co2e_non_co2_${i}__Mg"
//      }).toList
    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("grossEmissionsCo2eAllGases") as s"gfw_gross_emissions_co2e_all_gases_${i}__Mg"
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

      $"data.totalGrossCumulAbovegroundRemovalsCo2" as "gfw_gross_cumulative_aboveground_co2_removals_2001-2019__Mg",
      $"data.totalGrossCumulBelowgroundRemovalsCo2" as "gfw_gross_cumulative_belowground_co2_removals_2001-2019__Mg",
      $"data.totalGrossCumulAboveBelowgroundRemovalsCo2" as "gfw_gross_cumulative_aboveground_belowground_co2_removals_2001-2019__Mg",
      $"data.totalGrossEmissionsCo2eCo2Only" as "gfw_gross_emissions_co2e_co2_only_2001-2019__Mg",
      $"data.totalGrossEmissionsCo2eNonCo2" as "gfw_gross_emissions_co2e_non_co2_2001-2019__Mg",
      $"data.totalGrossEmissionsCo2eAllGases" as "gfw_gross_emissions_co2e_all_gases_2001-2019__Mg",
      $"data.totalNetFluxCo2" as "gfw_net_flux_co2e_2001-2019__Mg",
      $"data.totalFluxModelExtentArea" as "gfw_flux_model_extent__ha"
    )

    df.select(
      cols ::: treecoverLossCols ::: abovegroundBiomassLossCols :::
//        GrossEmissionsCo2eCo2OnlyCols ::: totalGrossEmissionsCo2eNonCo2Cols :::
        totalGrossEmissionsCo2eAllGasesCols: _*
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

//    val GrossEmissionsCo2eCo2OnlyCols =
//      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
//        sum(s"gfw_gross_emissions_co2e_co2_only_${i}__Mg") as s"gfw_gross_emissions_co2e_co2_only_${i}__Mg"
//      }).toList
//    val totalGrossEmissionsCo2eNonCo2Cols =
//      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
//        sum(s"gfw_gross_emissions_co2e_non_co2_${i}__Mg") as s"gfw_gross_emissions_co2e_non_co2_${i}__Mg"
//      }).toList
    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"gfw_gross_emissions_co2e_all_gases_${i}__Mg") as s"gfw_gross_emissions_co2e_all_gases_${i}__Mg"
      }).toList

    val cols = List(
      sum("area__ha") as "area__ha",
      sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
      sum("umd_tree_cover_extent_2010__ha") as "umd_tree_cover_extent_2010__ha",
      sum("umd_tree_cover_gain_2000-2012__ha") as "umd_tree_cover_gain_2000-2012__ha",
      sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",

      sum("gfw_gross_cumulative_aboveground_co2_removals_2001-2019__Mg")
        as "gfw_gross_cumulative_aboveground_co2_removals_2001-2019__Mg",
      sum("gfw_gross_cumulative_belowground_co2_removals_2001-2019__Mg")
        as "gfw_gross_cumulative_belowground_co2_removals_2001-2019__Mg",
      sum("gfw_gross_cumulative_aboveground_belowground_co2_removals_2001-2019__Mg")
        as "gfw_gross_cumulative_aboveground_belowground_co2_removals_2001-2019__Mg",
      sum("gfw_gross_emissions_co2e_co2_only_2001-2019__Mg")
        as "gfw_gross_emissions_co2e_co2_only_2001-2019__Mg",
      sum("gfw_gross_emissions_co2e_non_co2_2001-2019__Mg")
        as "gfw_gross_emissions_co2e_non_co2_2001-2019__Mg",
      sum("gfw_gross_emissions_co2e_all_gases_2001-2019__Mg")
        as "gfw_gross_emissions_co2e_all_gases_2001-2019__Mg",
      sum("gfw_net_flux_co2e_2001-2019__Mg") as "gfw_net_flux_co2e_2001-2019__Mg",
      sum("gfw_flux_model_extent__ha") as "gfw_flux_model_extent__ha"
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
        cols.tail ::: treecoverLossCols ::: abovegroundBiomassLossCols :::
//          GrossEmissionsCo2eCo2OnlyCols ::: totalGrossEmissionsCo2eNonCo2Cols :::
          totalGrossEmissionsCo2eAllGasesCols: _*
      )

  }

}
