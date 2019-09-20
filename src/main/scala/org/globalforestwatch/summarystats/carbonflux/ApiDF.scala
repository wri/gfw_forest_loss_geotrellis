package org.globalforestwatch.summarystats.carbonflux

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

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
      $"data_group.lossYear" as "loss_year",
      $"data_group.threshold" as "threshold",
      $"data_group.isGain" as "gain",
      $"data_group.isLoss" as "loss",
      $"data_group.mangroveBiomassExtent" as "mangroves",
      $"data_group.drivers" as "tcs_drivers",
      $"data_group.ecozones" as "ecozones",
      $"data_group.landRights" as "gfw_land_rights",
      $"data_group.wdpa" as "wdpa_protected_areas",
      $"data_group.intactForestLandscapes" as "intact_forest_landscapes",
      $"data_group.plantations" as "gfw_plantations",
      $"data_group.intactPrimaryForest" as "intact_primary_forest",
      $"data_group.peatlandsFlux" as "peatlands_flux",
      $"data.treecoverLoss" as "treecover_loss__ha",
      $"data.biomassLoss" as "aboveground_biomass_loss__Mg",
      $"data.grossEmissionsCo2eCo2Only" as "gross_emissions_co2e_co2_only__Mg",
      $"data.grossEmissionsCo2eNoneCo2" as "gross_emissions_co2e_none_co2__Mg",
      $"data.grossEmissionsCo2e" as "gross_emissions_co2e__Mg",
      $"data.agcEmisYear" as "aboveground_carbon_emissions__Mg",
      $"data.bgcEmisYear" as "belowground_carbon_emissions__Mg",
      $"data.deadwoodCarbonEmisYear" as "deadwood_wood_carbon_emissions__Mg",
      $"data.litterCarbonEmisYear" as "litter_carbon_emissions__Mg",
      $"data.soilCarbonEmisYear" as "soil_carbon_emissions__Mg",
      $"data.carbonEmisYear" as "total_carbon_emissions__Mg",
      $"data.totalExtent2000" as "treecover_extent_2000__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
      $"data.totalGrossAnnualRemovalsCarbon" as "gross_annual_removals_carbon__Mg",
      $"data.totalGrossCumulRemovalsCarbon" as "gross_annual_cumulative_removals_carbon__Mg",
      $"data.totalNetFluxCo2" as "net_flux_co2__Mg",
      $"data.totalAgc2000" as "aboveground_carbon_stock_2000__Mg",
      $"data.totalBgc2000" as "belowground_carbon_stock_2000__Mg",
      $"data.totalDeadwoodCarbon2000" as "deadwood_carbon_stock_2000__Mg",
      $"data.totalLitterCarbon2000" as "littler_carbon_stock_2000__Mg",
      $"data.totalSoil2000" as "soil_carbon_stock_2000__Mg",
      $"data.totalCarbon2000" as "total_carbon_stock_2000__Mg"
    )
  }

}
