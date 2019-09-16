package org.globalforestwatch.summarystats.carbonflux

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm2ApiDF {

  def nestYearData(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.select(
      $"iso",
      $"adm1",
      $"adm2",
      $"threshold",
      $"gain",
      $"mangroves",
      $"tcs",
      $"ecozone",
      $"land_right",
      $"wdpa",
      $"ifl",
      $"plantations",
      $"primary_forest",
      $"extent_2000",
      $"total_area",
      $"total_biomass",
      $"avg_biomass_per_ha" * $"extent_2000" as "weighted_biomass_per_ha",
      $"gross_annual_removals_carbon",
      $"avg_gross_annual_removals_carbon_ha" * $"extent_2000" as "weighted_gross_annual_removals_carbon_ha",
      $"gross_cumul_removals_carbon",
      $"avg_gross_cumul_removals_carbon_ha" * $"extent_2000" as "weighted_gross_cumul_removals_carbon_ha",
      $"net_flux_co2",
      $"avg_net_flux_co2_ha" * $"extent_2000" as "weighted_net_flux_co2_ha",
      $"agc_emissions_year",
      $"avg_agc_emissions_year" * $"extent_2000" as "weighted_agc_emissions_year",
      $"bgc_emissions_year",
      $"avg_bgc_emissions_year" * $"extent_2000" as "weighted_bgc_emissions_year",
      $"deadwood_carbon_emissions_year",
      $"avg_deadwood_carbon_emissions_year" * $"extent_2000" as "weighted_deadwood_carbon_emissions_year",
      $"litter_carbon_emissions_year",
      $"avg_litter_carbon_emissions_year" * $"extent_2000" as "weighted_litter_carbon_emissions_year",
      $"soil_carbon_emissions_year",
      $"avg_soil_carbon_emissions_year" * $"extent_2000" as "weighted_soil_carbon_emissions_year",
      $"total_carbon_emissions_year",
      $"avg_carbon_emissions_year" * $"extent_2000" as "weighted_carbon_emissions_year",
      $"agc_2000",
      $"avg_agc_2000" * $"extent_2000" as "weighted_agc_2000",
      $"bgc_2000",
      $"avg_bgc_2000" * $"extent_2000" as "weighted_bgc_2000",
      $"deadwood_carbon_2000",
      $"avg_deadwood_carbon_2000" * $"extent_2000" as "weighted_deadwood_carbon_2000",
      $"litter_carbon_2000",
      $"avg_litter_carbon_2000" * $"extent_2000" as "weighted_litter_carbon_2000",
      $"soil_2000_year",
      $"avg_soil_carbon_2000" * $"extent_2000" as "weighted_soil_carbon_2000",
      $"total_carbon_2000",
      $"avg_carbon_2000" * $"extent_2000" as "weighted_carbon_2000",
      $"gross_emissions_co2",
      $"avg_gross_emissions_co2" * $"extent_2000" as "weighted_gross_emissions_co2",
      array(
        struct(
          $"year_2001" as "year",
          $"area_loss_2001" as "area_loss",
          $"biomass_loss_2001" as "biomass_loss",
          $"gross_emissions_co2_2001" as "carbon_emissions"
        ),
        struct(
          $"year_2002" as "year",
          $"area_loss_2002" as "area_loss",
          $"biomass_loss_2002" as "biomass_loss",
          $"gross_emissions_co2_2002" as "carbon_emissions"
        ),
        struct(
          $"year_2003" as "year",
          $"area_loss_2003" as "area_loss",
          $"biomass_loss_2003" as "biomass_loss",
          $"gross_emissions_co2_2003" as "carbon_emissions"
        ),
        struct(
          $"year_2004" as "year",
          $"area_loss_2004" as "area_loss",
          $"biomass_loss_2004" as "biomass_loss",
          $"gross_emissions_co2_2004" as "carbon_emissions"
        ),
        struct(
          $"year_2005" as "year",
          $"area_loss_2005" as "area_loss",
          $"biomass_loss_2005" as "biomass_loss",
          $"gross_emissions_co2_2005" as "carbon_emissions"
        ),
        struct(
          $"year_2006" as "year",
          $"area_loss_2006" as "area_loss",
          $"biomass_loss_2006" as "biomass_loss",
          $"gross_emissions_co2_2006" as "carbon_emissions"
        ),
        struct(
          $"year_2007" as "year",
          $"area_loss_2007" as "area_loss",
          $"biomass_loss_2007" as "biomass_loss",
          $"gross_emissions_co2_2007" as "carbon_emissions"
        ),
        struct(
          $"year_2008" as "year",
          $"area_loss_2008" as "area_loss",
          $"biomass_loss_2008" as "biomass_loss",
          $"gross_emissions_co2_2008" as "carbon_emissions"
        ),
        struct(
          $"year_2009" as "year",
          $"area_loss_2009" as "area_loss",
          $"biomass_loss_2009" as "biomass_loss",
          $"gross_emissions_co2_2009" as "carbon_emissions"
        ),
        struct(
          $"year_2010" as "year",
          $"area_loss_2010" as "area_loss",
          $"biomass_loss_2010" as "biomass_loss",
          $"gross_emissions_co2_2010" as "carbon_emissions"
        ),
        struct(
          $"year_2011" as "year",
          $"area_loss_2011" as "area_loss",
          $"biomass_loss_2011" as "biomass_loss",
          $"gross_emissions_co2_2011" as "carbon_emissions"
        ),
        struct(
          $"year_2012" as "year",
          $"area_loss_2012" as "area_loss",
          $"biomass_loss_2012" as "biomass_loss",
          $"gross_emissions_co2_2012" as "carbon_emissions"
        ),
        struct(
          $"year_2013" as "year",
          $"area_loss_2013" as "area_loss",
          $"biomass_loss_2013" as "biomass_loss",
          $"gross_emissions_co2_2013" as "carbon_emissions"
        ),
        struct(
          $"year_2014" as "year",
          $"area_loss_2014" as "area_loss",
          $"biomass_loss_2014" as "biomass_loss",
          $"gross_emissions_co2_2014" as "carbon_emissions"
        ),
        struct(
          $"year_2015" as "year",
          $"area_loss_2015" as "area_loss",
          $"biomass_loss_2015" as "biomass_loss",
          $"gross_emissions_co2_2015" as "carbon_emissions"
        ),
        struct(
          $"year_2016" as "year",
          $"area_loss_2016" as "area_loss",
          $"biomass_loss_2016" as "biomass_loss",
          $"gross_emissions_co2_2016" as "carbon_emissions"
        ),
        struct(
          $"year_2017" as "year",
          $"area_loss_2017" as "area_loss",
          $"biomass_loss_2017" as "biomass_loss",
          $"gross_emissions_co2_2017" as "carbon_emissions"
        ),
        struct(
          $"year_2018" as "year",
          $"area_loss_2018" as "area_loss",
          $"biomass_loss_2018" as "biomass_loss",
          $"gross_emissions_co2_2018" as "carbon_emissions"
        )
      ) as "year_data"
    )

  }
}
