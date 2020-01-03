package org.globalforestwatch.summarystats.carbonflux

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.{length, max, sum, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object CarbonFluxDF {

  val contextualLayers: List[String] = List(
    "treecover_density__threshold",
    "is__treecover_loss_2000-2015",
    "is__treecover_gain_2000-2012",
    "is__mangrove",
    "tcs_driver__type",
    "ecozone__name",
    "is__gfw_land_right",
    "wdpa_protected_area__iucn_cat",
    "intact_forest_landscape__year",
    "gfw_plantation__type",
    "is__intact_primary_forest",
    "is__peatlands_flux"
  )

  private def setZeroNull(column: Column): Column = when(column =!= 0.0, column)

  def unpackValues(df: DataFrame): DataFrame = {

    implicit val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "dataGroup", "data"))

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"dataGroup.lossYear" as "treecover_loss__year",
      $"dataGroup.threshold" as "treecover_density__threshold",
      $"dataGroup.isGain" as "is__treecover_gain_2000-2012",
      $"dataGroup.isLoss" as "is__treecover_loss_2000-2015",
      $"dataGroup.mangroveBiomassExtent" as "is__mangrove",
      $"dataGroup.drivers" as "tcs_driver__type",
      $"dataGroup.ecozones" as "ecozone__name",
      $"dataGroup.landRights" as "is__gfw_land_right",
      $"dataGroup.wdpa" as "wdpa_protected_area__iucn_cat",
      $"dataGroup.intactForestLandscapes" as "intact_forest_landscape__year",
      $"dataGroup.plantations" as "gfw_plantation__type",
      $"dataGroup.intactPrimaryForest" as "is__intact_primary_forest",
      $"dataGroup.peatlandsFlux" as "is__peatlands_flux",
      $"data.treecoverLoss" as "treecover_loss__ha",
      $"data.biomassLoss" as "aboveground_biomass_loss__Mg",
      $"data.grossEmissionsCo2eCo2Only" as "gross_emissions_co2e_co2_only__Mg",
      $"data.grossEmissionsCo2eNoneCo2" as "gross_emissions_co2e_non_co2__Mg",
      $"data.grossEmissionsCo2e" as "gross_emissions_co2e_all_gases__Mg",
      $"data.agcEmisYear" as "aboveground_carbon_stock_in_emissions_year__Mg",
      $"data.bgcEmisYear" as "belowground_carbon_stock_in_emissions_year__Mg",
      $"data.deadwoodCarbonEmisYear" as "deadwood_carbon_stock_in_emissions_year__Mg",
      $"data.litterCarbonEmisYear" as "litter_carbon_stock_in_emissions_year__Mg",
      $"data.soilCarbonEmisYear" as "soil_carbon_stock_in_emissions_year__Mg",
      $"data.carbonEmisYear" as "total_carbon_stock_in_emissions_year__Mg",
      $"data.treecoverExtent2000" as "treecover_extent_2000__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
      $"data.totalGrossAnnualRemovalsCarbon" as "gross_annual_biomass_removals_2001-2015__Mg",
      $"data.totalGrossCumulRemovalsCarbon" as "gross_cumulative_co2_removals_2001-2015__Mg",
      $"data.totalNetFluxCo2" as "net_flux_co2_2001-2015__Mg",
      $"data.totalAgc2000" as "aboveground_carbon_stock_2000__Mg",
      $"data.totalBgc2000" as "belowground_carbon_stock_2000__Mg",
      $"data.totalDeadwoodCarbon2000" as "deadwood_carbon_stock_2000__Mg",
      $"data.totalLitterCarbon2000" as "litter_carbon_stock_2000__Mg",
      $"data.totalSoil2000" as "soil_carbon_stock_2000__Mg",
      $"data.totalCarbon2000" as "total_carbon_stock_2000__Mg"
    )
  }

  def aggSummary(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("area__ha") as "area__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("gross_annual_biomass_removals_2001-2015__Mg") as "gross_annual_biomass_removals_2001-2015__Mg",
        sum("gross_cumulative_co2_removals_2001-2015__Mg") as "gross_cumulative_co2_removals_2001-2015__Mg",
        sum("net_flux_co2_2001-2015__Mg") as "net_flux_co2_2001-2015__Mg",
        sum("aboveground_carbon_stock_2000__Mg") as "aboveground_carbon_stock_2000__Mg",
        sum("belowground_carbon_stock_2000__Mg") as "belowground_carbon_stock_2000__Mg",
        sum("deadwood_carbon_stock_2000__Mg") as "deadwood_carbon_stock_2000__Mg",
        sum("litter_carbon_stock_2000__Mg") as "litter_carbon_stock_2000__Mg",
        sum("soil_carbon_stock_2000__Mg") as "soil_carbon_stock_2000__Mg",
        sum("total_carbon_stock_2000__Mg") as "total_carbon_stock_2000__Mg",
        sum("treecover_loss__ha") as "treecover_loss_2001-2015__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2001-2015__Mg",
        sum("gross_emissions_co2e_co2_only__Mg") as "gross_emissions_co2e_co2_only_2001-2015__Mg",
        sum("gross_emissions_co2e_non_co2__Mg") as "gross_emissions_co2e_non_co2_2001-2015__Mg",
        sum("gross_emissions_co2e_all_gases__Mg") as "gross_emissions_co2e_all_gases_2001-2015__Mg"
      )
  }

  def aggSummary2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("area__ha") as "area__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("gross_annual_biomass_removals_2001-2015__Mg") as "gross_annual_biomass_removals_2001-2015__Mg",
        sum("gross_cumulative_co2_removals_2001-2015__Mg") as "gross_cumulative_co2_removals_2001-2015__Mg",
        sum("net_flux_co2_2001-2015__Mg") as "net_flux_co2_2001-2015__Mg",
        sum("aboveground_carbon_stock_2000__Mg") as "aboveground_carbon_stock_2000__Mg",
        sum("belowground_carbon_stock_2000__Mg") as "belowground_carbon_stock_2000__Mg",
        sum("deadwood_carbon_stock_2000__Mg") as "deadwood_carbon_stock_2000__Mg",
        sum("litter_carbon_stock_2000__Mg") as "litter_carbon_stock_2000__Mg",
        sum("soil_carbon_stock_2000__Mg") as "soil_carbon_stock_2000__Mg",
        sum("total_carbon_stock_2000__Mg") as "total_carbon_stock_2000__Mg",
        sum("treecover_loss_2001-2015__ha") as "treecover_loss_2001-2015__ha",
        sum("aboveground_biomass_loss_2001-2015__Mg") as "aboveground_biomass_loss_2001-2015__Mg",
        sum("gross_emissions_co2e_co2_only_2001-2015__Mg") as "gross_emissions_co2e_co2_only_2001-2015__Mg",
        sum("gross_emissions_co2e_non_co2_2001-2015__Mg") as "gross_emissions_co2e_non_co2_2001-2015__Mg",
        sum("gross_emissions_co2e_all_gases_2001-2015__Mg") as "gross_emissions_co2e_all_gases_2001-2015__Mg"
      )
  }

  def aggChange(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(
        groupByCols.head,
        groupByCols.tail ::: List("treecover_loss__year") ::: contextualLayers: _*
      )
      .agg(
        sum("treecover_loss__ha") as "treecover_loss__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss__Mg",
        sum("gross_emissions_co2e_co2_only__Mg") as "gross_emissions_co2e_co2_only__Mg",
        sum("gross_emissions_co2e_non_co2__Mg") as "gross_emissions_co2e_non_co2__Mg",
        sum("gross_emissions_co2e_all_gases__Mg") as "gross_emissions_co2e_all_gases__Mg",
        sum("aboveground_carbon_stock_in_emissions_year__Mg") as "aboveground_carbon_stock_in_emissions_year__Mg",
        sum("belowground_carbon_stock_in_emissions_year__Mg") as "belowground_carbon_stock_in_emissions_year__Mg",
        sum("deadwood_carbon_stock_in_emissions_year__Mg") as "deadwood_carbon_stock_in_emissions_year__Mg",
        sum("litter_carbon_stock_in_emissions_year__Mg") as "litter_carbon_stock_in_emissions_year__Mg",
        sum("soil_carbon_stock_in_emissions_year__Mg") as "soil_carbon_stock_in_emissions_year__Mg",
        sum("total_carbon_stock_in_emissions_year__Mg") as "total_carbon_stock_in_emissions_year__Mg"
      )
  }

  def whitelist(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        max($"is__treecover_loss_2000-2015") as "is__treecover_loss_2000-2015",
        max($"is__treecover_gain_2000-2012") as "is__treecover_gain_2000-2012",
        max($"is__mangrove") as "is__mangrove",
        max($"tcs_driver__type") as "tcs_driver__type",
        max($"ecozone__name") as "ecozone__name",
        max($"is__gfw_land_right") as "is__gfw_land_right",
        max($"wdpa_protected_area__iucn_cat") as "wdpa_protected_area__iucn_cat",
        max($"intact_forest_landscape__year") as "intact_forest_landscape__year",
        max($"gfw_plantation__type") as "gfw_plantation__type",
        max($"is__intact_primary_forest") as "is__intact_primary_forest",
        max($"is__peatlands_flux") as "is__peatlands_flux"
      )
  }

  def whitelist2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        max($"is__treecover_loss_2000-2015") as "is__treecover_loss_2000-2015",
        max($"is__treecover_gain_2000-2012") as "is__treecover_gain_2000-2012",
        max($"is__mangrove") as "is__mangrove",
        max(length($"tcs_driver__type")).cast("boolean") as "tcs_driver__type",
        max(length($"ecozone__name")).cast("boolean") as "ecozone__name",
        max($"is__gfw_land_right") as "is__gfw_land_right",
        max(length($"wdpa_protected_area__iucn_cat"))
          .cast("boolean") as "wdpa_protected_area__iucn_cat",
        max(length($"intact_forest_landscape__year"))
          .cast("boolean") as "intact_forest_landscape__year",
        max(length($"gfw_plantation__type"))
          .cast("boolean") as "gfw_plantation__type",
        max($"is__intact_primary_forest") as "is__intact_primary_forest",
        max($"is__peatlands_flux") as "is__peatlands_flux"
      )
  }
}
