package org.globalforestwatch.summarystats.annualupdate_minimal

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.{DataFrame, SparkSession}

object ApiDF {

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"data_group.lossYear" as "loss_year",
      $"data_group.threshold" as "threshold",
      $"data_group.drivers" as "tcs",
      $"data_group.globalLandCover" as "global_land_cover",
      $"data_group.primaryForest" as "primary_forest",
      //      $"data_group.idnPrimaryForest" as "idn_primary_forest",
      //      $"data_group.erosion" as "erosion",
      //      $"data_group.biodiversitySignificance" as "biodiversity_significance",
      //      $"data_group.biodiversityIntactness" as "biodiversity_intactness",
      $"data_group.wdpa" as "wdpa",
      $"data_group.aze" as "aze",
      $"data_group.plantations" as "plantations",
      //      $"data_group.riverBasins" as "river_basin",
      //      $"data_group.ecozones" as "ecozone",
      //      $"data_group.urbanWatersheds" as "urban_watershed",
      $"data_group.mangroves1996" as "mangroves_1996",
      $"data_group.mangroves2016" as "mangroves_2016",
      //      $"data_group.waterStress" as "water_stress",
      $"data_group.intactForestLandscapes" as "ifl",
      //      $"data_group.endemicBirdAreas" as "endemic_bird_area",
      $"data_group.tigerLandscapes" as "tiger_cl",
      $"data_group.landmark" as "landmark",
      $"data_group.landRights" as "land_right",
      $"data_group.keyBiodiversityAreas" as "kba",
      $"data_group.mining" as "mining",
      //      $"data_group.rspo" as "rspo",
      $"data_group.peatlands" as "idn_mys_peatlands",
      $"data_group.oilPalm" as "oil_palm",
      $"data_group.idnForestMoratorium" as "idn_forest_moratorium",
      //      $"data_group.idnLandCover" as "idn_land_cover",
      //      $"data_group.mexProtectedAreas" as "mex_protected_areas",
      //      $"data_group.mexPaymentForEcosystemServices" as "mex_pes",
      //      $"data_group.mexForestZoning" as "mex_forest_zoning",
      //      $"data_group.perProductionForest" as "per_production_forest",
      //      $"data_group.perProtectedAreas" as "per_protected_area",
      //      $"data_group.perForestConcessions" as "per_forest_concession",
      //      $"data_group.braBiomes" as "bra_biomes",
      $"data_group.woodFiber" as "wood_fiber",
      $"data_group.resourceRights" as "resource_right",
      $"data_group.logging" as "managed_forests",
      //      $"data_group.oilGas" as "oil_gas",
      $"data.extent2000" as "treecover_extent_2000_ha",
      $"data.extent2010" as "treecover_extent_2010_ha",
      $"data.totalArea" as "total_area_ha",
      $"data.totalGainArea" as "total_gain_2000-2012_ha",
      $"data.totalBiomass" as "total_biomass_stock_2000_Mt",
      $"data.totalCo2" as "total_co2_stock_Mt",
      //      $"data.totalMangroveBiomass" as "total_mangrove_biomass_stock_Mt",
      //      $"data.totalMangroveCo2" as "total_mangrove_co2_stock_Mt",
      $"data.areaLoss" as "area_loss_ha",
      $"data.biomassLoss" as "biomass_loss_Mt",
      $"data.co2Emissions" as "co2_emissions_Mt"
      //      $"data.mangroveBiomassLoss" as "mangrove_biomass_loss_Mt",
      //      $"data.mangroveCo2Emissions" as "mangrove_co2_emissions_Mt"
    )
  }
}
