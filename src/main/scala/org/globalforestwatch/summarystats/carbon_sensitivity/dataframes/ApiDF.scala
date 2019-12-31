package org.globalforestwatch.summarystats.carbon_sensitivity.dataframes

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ApiDF {

  private def setZeroNull(column: Column): Column = when(column =!= 0.0, column)

  def unpackValues(df: DataFrame): DataFrame = {

    implicit val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"data_group.lossYear" as "treecover_loss__year",
      $"data_group.threshold" as "treecover_density__threshold",
      $"data_group.isGain" as "is__treecover_gain_2000-2012",
      $"data_group.isLoss" as "is__treecover_loss_2000-2015",
      $"data_group.mangroveBiomassExtent" as "is__mangrove",
      $"data_group.drivers" as "tcs_driver__type",
      $"data_group.ecozones" as "ecozone__name",
      $"data_group.landRights" as "is__gfw_land_right",
      $"data_group.wdpa" as "wdpa_protected_area__iucn_cat",
      $"data_group.intactForestLandscapes" as "intact_forest_landscape__year",
      $"data_group.plantations" as "gfw_plantation__type",
      $"data_group.intactPrimaryForest" as "is__intact_primary_forest",
      $"data_group.peatlandsFlux" as "is__peatlands_flux",
      $"data_group.forestAgeCategory" as "forest_age_category__cat",
      $"data_group.jplAGBextent" as "is__jplAGBextent",
      $"data_group.FIAregionsUSextent" as "FIAregionsUSextent__region",
      $"data.treecoverLoss" as "treecover_loss__ha",
      $"data.biomassLoss" as "aboveground_biomass_loss__Mg",
      $"data.grossEmissionsCo2eCo2Only" as "gross_emissions_co2e_co2_only__Mg",
      $"data.grossEmissionsCo2eNoneCo2" as "gross_emissions_co2e_non_co2__Mg",
      $"data.grossEmissionsCo2e" as "gross_emissions_co2e_all_gases__Mg",
      $"data.agcEmisYear" as "aboveground_carbon_stock_in_emissions_year__Mg",
//      $"data.bgcEmisYear" as "belowground_carbon_stock_in_emissions_year__Mg",
//      $"data.deadwoodCarbonEmisYear" as "deadwood_carbon_stock_in_emissions_year__Mg",
//      $"data.litterCarbonEmisYear" as "litter_carbon_stock_in_emissions_year__Mg",
      $"data.soilCarbonEmisYear" as "soil_carbon_stock_in_emissions_year__Mg",
//      $"data.carbonEmisYear" as "total_carbon_stock_in_emissions_year__Mg",
      $"data.treecoverExtent2000" as "treecover_extent_2000__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
//      $"data.totalGrossAnnualRemovalsCarbon" as "gross_annual_biomass_removals_2001-2015__Mg",
      $"data.totalGrossCumulRemovalsCarbon" as "gross_cumulative_co2_removals_2001-2015__Mg",
      $"data.totalNetFluxCo2" as "net_flux_co2_2001-2015__Mg",
      $"data.totalAgc2000" as "aboveground_carbon_stock_2000__Mg",
//      $"data.totalBgc2000" as "belowground_carbon_stock_2000__Mg",
//      $"data.totalDeadwoodCarbon2000" as "deadwood_carbon_stock_2000__Mg",
//      $"data.totalLitterCarbon2000" as "litter_carbon_stock_2000__Mg",
      $"data.totalSoil2000" as "soil_carbon_stock_2000__Mg"
//      $"data.totalCarbon2000" as "total_carbon_stock_2000__Mg"
    )
  }

}
