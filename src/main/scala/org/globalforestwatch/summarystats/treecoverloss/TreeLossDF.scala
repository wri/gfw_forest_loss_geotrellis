package org.globalforestwatch.summarystats.treecoverloss

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object TreeLossDF {

  val treecoverLossMinYear = 2001
  val treecoverLossMaxYear = 2023

  def unpackValues(carbonPools: Boolean, simpleAGBEmis: Boolean, emisGasAnnual: Boolean)(df: DataFrame): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("treecoverLoss") as s"umd_tree_cover_loss_${i}__ha"
      }).toList

    val abovegroundBiomassLossCols = if (simpleAGBEmis) {
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("biomassLoss") as s"whrc_aboveground_biomass_loss_${i}__Mg"
      }).toList
    } else {
      List()
    }

    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("grossEmissionsCo2eAllGases") as s"gfw_forest_carbon_gross_emissions_all_gases_${i}__Mg_CO2e"
      }).toList

    val cols = List(
      $"id.featureId" as "feature__id",
      $"data_group.threshold" as "umd_tree_cover_density__threshold",
      $"data_group.tcdYear" as "umd_tree_cover_extent__year",
      $"data_group.isPrimaryForest" as "is__umd_regional_primary_forest_2001",
      $"data_group.isPlantations" as "is__gfw_plantations",
      $"data_group.isGlobalPeat" as "is__global_peat",
      $"data_group.tclDriverClass" as "tcl_driver__class",
      $"data_group.isTreeCoverLossFire" as "is__tree_cover_loss_from_fires",
      $"data.treecoverExtent2000" as "umd_tree_cover_extent_2000__ha",
      $"data.treecoverExtent2010" as "umd_tree_cover_extent_2010__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalGainArea" as "umd_tree_cover_gain__ha",
      $"data.totalBiomass" as "whrc_aboveground_biomass_stock_2000__Mg",
      $"data.avgBiomass" as "avg_whrc_aboveground_biomass_stock_2000__Mg_ha-1",
      $"data.totalGrossCumulAbovegroundRemovalsCo2" as s"gfw_forest_carbon_gross_removals_aboveground_2001_${treecoverLossMaxYear}__Mg_CO2",
      $"data.totalGrossCumulBelowgroundRemovalsCo2" as s"gfw_forest_carbon_gross_removals_belowground_2001_${treecoverLossMaxYear}__Mg_CO2",
      $"data.totalGrossCumulAboveBelowgroundRemovalsCo2" as s"gfw_forest_carbon_gross_removals_2001_${treecoverLossMaxYear}__Mg_CO2",
      $"data.totalGrossEmissionsCo2eCo2Only" as s"gfw_forest_carbon_gross_emissions_CO2_2001_${treecoverLossMaxYear}__Mg_CO2",
      $"data.totalGrossEmissionsCo2eCh4" as s"gfw_forest_carbon_gross_emissions_CH4_2001_${treecoverLossMaxYear}__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eN2o" as s"gfw_forest_carbon_gross_emissions_N2O_2001_${treecoverLossMaxYear}__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eAllGases" as s"gfw_forest_carbon_gross_emissions_all_gases_2001_${treecoverLossMaxYear}__Mg_CO2e",
      $"data.totalNetFluxCo2" as s"gfw_forest_carbon_net_flux_2001_${treecoverLossMaxYear}__Mg_CO2e",
      $"data.totalFluxModelExtentArea" as "gfw_flux_model_extent__ha"
    )

    val carbonPoolCols = if (carbonPools) {
      List(
        $"data.totalAgc2000" as "gfw_aboveground_carbon_stock_2000__Mg_C",
        $"data.totalBgc2000" as "gfw_belowground_carbon_stock_2000__Mg_C",
        $"data.totalSoilCarbon2000" as "gfw_soil_carbon_stock_2000__Mg_C"
      )
    } else {
      List()
    }

    val totalGrossEmissionsCo2Co2OnlyCols = if (emisGasAnnual) {
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("grossEmissionsCo2eCo2Only") as s"gfw_forest_carbon_gross_emissions_CO2_${i}__Mg_CO2e"
      }).toList
    } else {
      List()
    }

    val totalGrossEmissionsCo2eCh4Cols = if (emisGasAnnual) {
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("grossEmissionsCo2eCh4") as s"gfw_forest_carbon_gross_emissions_CH4_${i}__Mg_CO2e"
      }).toList
    } else {
      List()
    }

    val totalGrossEmissionsCo2eN2oCols = if (emisGasAnnual) {
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        $"data.lossYear"
          .getItem(i)
          .getItem("grossEmissionsCo2eN2o") as s"gfw_forest_carbon_gross_emissions_N2O_${i}__Mg_CO2e"
      }).toList
    } else {
      List()
    }


    df.select(
      cols ::: carbonPoolCols ::: treecoverLossCols ::: abovegroundBiomassLossCols
        ::: totalGrossEmissionsCo2eAllGasesCols :::
        totalGrossEmissionsCo2Co2OnlyCols ::: totalGrossEmissionsCo2eCh4Cols ::: totalGrossEmissionsCo2eN2oCols : _*
    )

  }

  def contextualLayerFilter(
                             includePrimaryForest: Boolean,
                             includePlantations: Boolean,
                             includeGlobalPeat: Boolean,
                             includeTclDriverClass: Boolean,
                             includeTreeCoverLossFires: Boolean,
                             carbonPools: Boolean,
                             simpleAGBEmis: Boolean,
                             emisGasAnnual: Boolean
                           )(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._



    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"umd_tree_cover_loss_${i}__ha") as s"umd_tree_cover_loss_${i}__ha"
      }).toList

    val abovegroundBiomassLossCols = if (simpleAGBEmis) {
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"whrc_aboveground_biomass_loss_${i}__Mg") as s"whrc_aboveground_biomass_loss_${i}__Mg"
      }).toList
      } else {
        List()
      }

    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"gfw_forest_carbon_gross_emissions_all_gases_${i}__Mg_CO2e") as s"gfw_forest_carbon_gross_emissions_all_gases_${i}__Mg_CO2e"
      }).toList

    val totalGrossEmissionsCo2Co2OnlyCols = if (emisGasAnnual) {
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"gfw_forest_carbon_gross_emissions_CO2_${i}__Mg_CO2e") as s"gfw_forest_carbon_gross_emissions_CO2_${i}__Mg_CO2e"
      }).toList
    } else {
      List()
    }

    val totalGrossEmissionsCo2eCh4Cols = if (emisGasAnnual) {
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"gfw_forest_carbon_gross_emissions_CH4_${i}__Mg_CO2e") as s"gfw_forest_carbon_gross_emissions_CH4_${i}__Mg_CO2e"
      }).toList
    } else {
      List()
    }

    val totalGrossEmissionsCo2eN2oCols = if (emisGasAnnual) {
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum(s"gfw_forest_carbon_gross_emissions_N2O_${i}__Mg_CO2e") as s"gfw_forest_carbon_gross_emissions_N2O_${i}__Mg_CO2e"
      }).toList
    } else {
      List()
    }

    val cols = List(
      sum("area__ha") as "area__ha",
      sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
      sum("umd_tree_cover_extent_2010__ha") as "umd_tree_cover_extent_2010__ha",
      sum("umd_tree_cover_gain__ha") as "umd_tree_cover_gain__ha",
      sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
      sum($"avg_whrc_aboveground_biomass_stock_2000__Mg_ha-1" * $"umd_tree_cover_extent_2000__ha") /
        sum($"umd_tree_cover_extent_2000__ha") as "avg_whrc_aboveground_biomass_density_2000__Mg_ha-1",

      sum(s"gfw_forest_carbon_gross_removals_aboveground_2001_${treecoverLossMaxYear}__Mg_CO2")
        as s"gfw_forest_carbon_gross_removals_aboveground_2001_${treecoverLossMaxYear}__Mg_CO2",
      sum(s"gfw_forest_carbon_gross_removals_belowground_2001_${treecoverLossMaxYear}__Mg_CO2")
        as s"gfw_forest_carbon_gross_removals_belowground_2001_${treecoverLossMaxYear}__Mg_CO2",
      sum(s"gfw_forest_carbon_gross_removals_2001_${treecoverLossMaxYear}__Mg_CO2")
        as s"gfw_forest_carbon_gross_removals_2001_${treecoverLossMaxYear}__Mg_CO2",
      sum(s"gfw_forest_carbon_gross_emissions_CO2_2001_${treecoverLossMaxYear}__Mg_CO2")
        as s"gfw_forest_carbon_gross_emissions_CO2_2001_${treecoverLossMaxYear}__Mg_CO2",
      sum(s"gfw_forest_carbon_gross_emissions_CH4_2001_${treecoverLossMaxYear}__Mg_CO2e")
        as s"gfw_forest_carbon_gross_emissions_CH4_2001_${treecoverLossMaxYear}__Mg_CO2e",
      sum(s"gfw_forest_carbon_gross_emissions_N2O_2001_${treecoverLossMaxYear}__Mg_CO2e")
        as s"gfw_forest_carbon_gross_emissions_N2O_2001_${treecoverLossMaxYear}__Mg_CO2e",
      sum(s"gfw_forest_carbon_gross_emissions_all_gases_2001_${treecoverLossMaxYear}__Mg_CO2e")
        as s"gfw_forest_carbon_gross_emissions_all_gases_2001_${treecoverLossMaxYear}__Mg_CO2e",
      sum(s"gfw_forest_carbon_net_flux_2001_${treecoverLossMaxYear}__Mg_CO2e")
        as s"gfw_forest_carbon_net_flux_2001_${treecoverLossMaxYear}__Mg_CO2e",
      sum("gfw_flux_model_extent__ha") as "gfw_flux_model_extent__ha"
    )

    val carbonPoolCols = if (carbonPools) {
      List(
        sum("gfw_aboveground_carbon_stock_2000__Mg_C") as "gfw_aboveground_carbon_stock_2000__Mg_C",
        sum("gfw_belowground_carbon_stock_2000__Mg_C") as "gfw_belowground_carbon_stock_2000__Mg_C",
        sum("gfw_soil_carbon_stock_2000__Mg_C") as "gfw_soil_carbon_stock_2000__Mg_C"
      )
    } else {
      List()
    }

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

    val ptGroupByCol = {
      if (includeGlobalPeat) List($"is__global_peat")
      else List()
    }

    val drGroupByCol = {
      if (includeTclDriverClass) List($"tcl_driver__class")
      else List()
    }

    val fiGroupByCol = {
      if (includeTreeCoverLossFires) List($"is__tree_cover_loss_from_fires")
      else List()
    }


    df.groupBy(groupByCols ::: pfGroupByCol ::: plGroupByCol ::: ptGroupByCol ::: drGroupByCol ::: fiGroupByCol : _*)
      .agg(
        cols.head,
        cols.tail ::: carbonPoolCols ::: treecoverLossCols ::: abovegroundBiomassLossCols
          ::: totalGrossEmissionsCo2eAllGasesCols :::
          totalGrossEmissionsCo2Co2OnlyCols ::: totalGrossEmissionsCo2eCh4Cols ::: totalGrossEmissionsCo2eN2oCols : _*
      )

  }



}
