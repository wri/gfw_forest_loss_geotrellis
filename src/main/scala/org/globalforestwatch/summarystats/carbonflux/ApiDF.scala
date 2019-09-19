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
      $"data_group.gain" as "gain",
      $"data_group.mangroveBiomassExtent" as "mangroves",
      $"data_group.drivers" as "tcs",
      $"data_group.ecozones" as "ecozone",
      $"data_group.landRights" as "land_right",
      $"data_group.wdpa" as "wdpa",
      $"data_group.intactForestLandscapes" as "ifl",
      $"data_group.plantations" as "plantations",
      $"data_group.intactPrimaryForest" as "intact_primary_forest",
      $"data_group.peatlandsFlux" as "peatlands_flux",
      $"data.areaLoss" as "area_loss_ha",
      $"data.biomassLoss" as "biomass_loss_Mt",
      $"data.grossEmissionsCo2eCo2Only" as "gross_emissions_co2e_co2_only_Mt",
      $"data.grossEmissionsCo2eNoneCo2" as "gross_emissions_co2e_none_co2_Mt",
      $"data.grossEmissionsCo2e" as "gross_emissions_co2e_Mt",
      $"data.agcEmisYear" as "agc_emissions_Mt",
      $"data.bgcEmisYear" as "bgc_emissions_Mt",
      $"data.deadwoodCarbonEmisYear" as "deadwood_wood_carbon_emissions_Mt",
      $"data.litterCarbonEmisYear" as "litter_carbon_emissions_Mt",
      $"data.soilCarbonEmisYear" as "soil_carbon_emissions_Mt",
      $"data.carbonEmisYear" as "carbon_emissions_Mt",
      $"data.totalExtent2000" as "total_extent_2000_ha",
      $"data.totalArea" as "total_area_ha",
      $"data.totalBiomass" as "total_biomass_Mt",
      $"data.totalGrossAnnualRemovalsCarbon" as "total_gross_annual_removals_carbon_Mt",
      $"data.totalGrossCumulRemovalsCarbon" as "total_gross_annual_cumulative_removals_carbon_Mt",
      $"data.totalNetFluxCo2" as "total_net_flux_co2_Mt",
      $"data.totalAgc2000" as "total_agc_2000_Mt",
      $"data.totalBgc2000" as "total_bgc_2000_Mt",
      $"data.totalDeadwoodCarbon2000" as "total_deadwood_carbon_2000_Mt",
      $"data.totalLitterCarbon2000" as "total_littler_carbon_2000_Mt",
      $"data.totalSoil2000" as "total_soil_2000_Mt",
      $"data.totalCarbon2000" as "total_carbon_2000_Mt"
      //    $"data.totalGrossEmissionsCo2eCo2Only" as "total_gross_emissions_co2e_co2_only_Mt",
      //    $"data.totalGrossEmissionsCo2eNoneCo2" as "total_gross_emissions_co2e_none_co2_Mt",
      //    $"data.totalGrossEmissionsCo2e" as "total_gross_emissions_co2e_Mt",
    )
  }

}
