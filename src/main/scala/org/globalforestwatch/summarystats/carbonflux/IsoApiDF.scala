package org.globalforestwatch.summarystats.carbonflux

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object IsoApiDF {

  def sumChange(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
        $"iso",
        $"threshold",
        $"gain",
        $"mangroves",
        $"tcs",
        $"ecozone",
        $"land_right",
        $"wdpa",
        $"ifl",
        $"plantations",
        $"intact_primary_forest",
        $"peatlands_flux"
      )
      .agg(
        sum("area_loss_ha") as "area_loss_ha",
        sum("biomass_loss_Mt") as "biomass_loss_Mt",
        sum("gross_emissions_co2e_co2_only_Mt") as "gross_emissions_co2e_co2_only_Mt",
        sum("gross_emissions_co2e_none_co2_Mt") as "gross_emissions_co2e_none_co2_Mt",
        sum("gross_emissions_co2e_Mt") as "gross_emissions_co2e_Mt",
        sum("agc_emissions_Mt") as "agc_emissions_Mt",
        sum("bgc_emissions_Mt") as "bgc_emissions_Mt",
        sum("deadwood_wood_carbon_emissions_Mt") as "deadwood_wood_carbon_emissions_Mt",
        sum("litter_carbon_emissions_Mt") as "litter_carbon_emissions_Mt",
        sum("soil_carbon_emissions_Mt") as "soil_carbon_emissions_Mt",
        sum("carbon_emissions_Mt") as "carbon_emissions_Mt"
      )
  }

  def sumArea(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
        $"iso",
        $"loss_year",
        $"threshold",
        $"gain",
        $"mangroves",
        $"tcs",
        $"ecozone",
        $"land_right",
        $"wdpa",
        $"ifl",
        $"plantations",
        $"intact_primary_forest",
        $"peatlands_flux"
      )
      .agg(
        sum("total_extent_2000_ha") as "total_extent_2000_ha",
        sum("total_area_ha") as "total_area_ha",
        sum("total_biomass_Mt") as "total_biomass_Mt",
        sum("total_gross_annual_removals_carbon_Mt") as "total_gross_annual_removals_carbon_Mt",
        sum("total_gross_annual_cumulative_removals_carbon_Mt") as "total_gross_annual_cumulative_removals_carbon_Mt",
        sum("total_net_flux_co2_Mt") as "total_net_flux_co2_Mt",
        sum("total_agc_2000_Mt") as "total_agc_2000_Mt",
        sum("total_bgc_2000_Mt") as "total_bgc_2000_Mt",
        sum("total_deadwood_carbon_2000_Mt") as "total_deadwood_carbon_2000_Mt",
        sum("total_littler_carbon_2000_Mt") as "total_littler_carbon_2000_Mt",
        sum("total_soil_2000_Mt") as "total_soil_2000_Mt",
        sum("total_carbon_2000_Mt") as "total_carbon_2000_Mt"
      )
  }
}
