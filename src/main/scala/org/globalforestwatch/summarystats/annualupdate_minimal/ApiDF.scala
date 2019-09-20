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
      $"data_group.drivers" as "tcs_drivers",
      $"data_group.globalLandCover" as "global_land_cover",
      $"data_group.primaryForest" as "regional_primary_forests",
      //      $"data_group.idnPrimaryForest" as "idn_primary_forests",
      //      $"data_group.erosion" as "erosion_risk",
      //      $"data_group.biodiversitySignificance" as "biodiversity_significance_top_10_perc",
      //      $"data_group.biodiversityIntactness" as "biodiversity_intactness_top_10_perc",
      $"data_group.wdpa" as "wdpa_protected_areas",
      $"data_group.aze" as "alliance_for_zero_extinction_sites",
      $"data_group.plantations" as "gfw_plantations",
      //      $"data_group.riverBasins" as "river_basins",
      //      $"data_group.ecozones" as "ecozones",
      //      $"data_group.urbanWatersheds" as "urban_water_intakes",
      $"data_group.mangroves1996" as "mangroves_1996",
      $"data_group.mangroves2016" as "mangroves_2016",
      //      $"data_group.waterStress" as "baseline_water_stress",
      $"data_group.intactForestLandscapes" as "intact_forest_landscapes",
      //      $"data_group.endemicBirdAreas" as "endemic_bird_areas",
      $"data_group.tigerLandscapes" as "tiger_conservation_landscapes",
      $"data_group.landmark" as "landmark",
      $"data_group.landRights" as "gfw_land_rights",
      $"data_group.keyBiodiversityAreas" as "key_biodiversity_areas",
      $"data_group.mining" as "gfw_mining",
      //      $"data_group.rspo" as "rspo_oil_palm",
      $"data_group.peatlands" as "peat_lands",
      $"data_group.oilPalm" as "gfw_oil_palm",
      $"data_group.idnForestMoratorium" as "idn_forest_moratorium",
      //      $"data_group.idnLandCover" as "idn_land_cover",
      //      $"data_group.mexProtectedAreas" as "mex_protected_areas",
      //      $"data_group.mexPaymentForEcosystemServices" as "mex_psa",
      //      $"data_group.mexForestZoning" as "mex_forest_zoning",
      //      $"data_group.perProductionForest" as "per_permanent_production_forests",
      //      $"data_group.perProtectedAreas" as "per_protected_areas",
      //      $"data_group.perForestConcessions" as "per_forest_concessions",
      //      $"data_group.braBiomes" as "bra_biomes",
      $"data_group.woodFiber" as "gfw_wood_fiber",
      $"data_group.resourceRights" as "gfw_resource_rights",
      $"data_group.logging" as "gfw_logging",
      //      $"data_group.oilGas" as "gfw_oil_gas",
      $"data.treecoverExtent2000" as "treecover_extent_2000__ha",
      $"data.treecoverExtent2010" as "treecover_extent_2010__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalGainArea" as "treecover_gain_2000-2012__ha",
      $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
      $"data.totalCo2" as "co2_stock__Mg",
      //      $"data.totalMangroveBiomass" as "mangrove_aboveground_biomass_stock__Mg",
      //      $"data.totalMangroveCo2" as "mangrove_co2_stock__Mg",
      $"data.treecoverLoss" as "treecover_loss__ha",
      $"data.biomassLoss" as "aboveground_biomass_loss__Mg",
      $"data.co2Emissions" as "co2_emissions__Mg"
      //      $"data.mangroveBiomassLoss" as "mangrove_aboveground_biomass_loss__Mg",
      //      $"data.mangroveCo2Emissions" as "mangrove_co2_emissions__Mg"
    )
  }
}
