package org.globalforestwatch.summarystats.carbonflux

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm2ApiDF {

  def sumChange(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.filter($"treecover_loss__year".isNotNull && $"treecover_loss__ha" > 0)
      .groupBy(
        $"iso",
        $"adm1",
        $"adm2",
        $"treecover_density__threshold",
        $"treecover_loss__year",
        $"is__treecover_gain",
        $"is__mangrove",
        $"tcs_driver__type",
        $"ecozone__name",
        $"is__gfw_land_right",
        $"wdpa_protected_area__iucn_cat",
        $"is__intact_forest_landscape",
        $"is__gfw_plantation",
        $"is__intact_primary_forest",
        $"peatlands_flux"
      )
      .agg(
        sum("treecover_loss__ha") as "treecover_loss__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss__Mg",
        sum("gross_emissions_co2e_co2_only__Mg") as "gross_emissions_co2e_co2_only__Mg",
        sum("gross_emissions_co2e_none_co2__Mg") as "gross_emissions_co2e_none_co2__Mg",
        sum("gross_emissions_co2e__Mg") as "gross_emissions_co2e__Mg",
        sum("aboveground_carbon_emissions__Mg") as "aboveground_carbon_emissions__Mg",
        sum("belowground_carbon_emissions__Mg") as "belowground_carbon_emissions__Mg",
        sum("deadwood_wood_carbon_emissions__Mg") as "deadwood_wood_carbon_emissions__Mg",
        sum("litter_carbon_emissions__Mg") as "litter_carbon_emissions__Mg",
        sum("soil_carbon_emissions__Mg") as "soil_carbon_emissions__Mg",
        sum("total_carbon_emissions__Mg") as "total_carbon_emissions__Mg"
      )
  }

  def sumArea(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
      $"iso",
      $"adm1",
      $"adm2",
      $"treecover_density__threshold",
      $"is__treecover_loss",
      $"is__treecover_gain",
      $"is__mangrove",
      $"tcs_driver__type",
      $"ecozone__name",
      $"is__gfw_land_right",
      $"wdpa_protected_area__iucn_cat",
      $"is__intact_forest_landscape",
      $"is__gfw_plantation",
      $"is__intact_primary_forest",
      $"peatlands_flux"
    )
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("area__ha") as "area__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("gross_annual_removals_carbon__Mg") as "gross_annual_removals_carbon__Mg",
        sum("gross_annual_cumulative_removals_carbon__Mg") as "gross_annual_cumulative_removals_carbon__Mg",
        sum("net_flux_co2__Mg") as "net_flux_co2__Mg",
        sum("aboveground_carbon_stock_2000__Mg") as "aboveground_carbon_stock_2000__Mg",
        sum("belowground_carbon_stock_2000__Mg") as "belowground_carbon_stock_2000__Mg",
        sum("deadwood_carbon_stock_2000__Mg") as "deadwood_carbon_stock_2000__Mg",
        sum("littler_carbon_stock_2000__Mg") as "littler_carbon_stock_2000__Mg",
        sum("soil_carbon_stock_2000__Mg") as "soil_carbon_stock_2000__Mg",
        sum("total_carbon_stock_2000__Mg") as "total_carbon_stock_2000__Mg"
      )
  }
}
