package org.globalforestwatch.summarystats.carbonflux

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm1ApiDF {

  def sumChange(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
        $"iso",
        $"adm1",
      $"treecover_density__threshold",
      $"treecover_loss__year",
      $"is__treecover_gain_2000-2012",
      $"is__mangrove",
      $"tcs_driver__type",
      $"ecozone__name",
      $"is__gfw_land_right",
      $"wdpa_protected_area__iucn_cat",
      $"intact_forest_landscape__year",
      $"gfw_plantation__type",
      $"is__intact_primary_forest",
      $"is__peatlands_flux"
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

  def sumArea(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
        $"iso",
        $"adm1",
      $"treecover_density__threshold",
      $"is__treecover_loss_2000-2015",
      $"is__treecover_gain_2000-2012",
      $"is__mangrove",
      $"tcs_driver__type",
      $"ecozone__name",
      $"is__gfw_land_right",
      $"wdpa_protected_area__iucn_cat",
      $"intact_forest_landscape__year",
      $"gfw_plantation__type",
      $"is__intact_primary_forest",
      $"is__peatlands_flux"
      )
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
}
