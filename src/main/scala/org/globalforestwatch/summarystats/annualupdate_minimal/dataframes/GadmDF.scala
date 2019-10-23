package org.globalforestwatch.summarystats.annualupdate_minimal.dataframes

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.{DataFrame, SparkSession}

object GadmDF {

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"data_group.lossYear" as "treecover_loss__year",
      $"data_group.threshold" as "treecover_density__threshold",
      $"data_group.drivers" as "tcs_driver__type",
      $"data_group.globalLandCover" as "global_land_cover__class",
      $"data_group.primaryForest" as "is__regional_primary_forest",
      //      $"data_group.idnPrimaryForest" as "is__idn_primary_forest",
      //      $"data_group.erosion" as "erosion_risk__level",
      //      $"data_group.biodiversitySignificance" as "is__biodiversity_significance_top_10_perc",
      //      $"data_group.biodiversityIntactness" as "is__biodiversity_intactness_top_10_perc",
      $"data_group.wdpa" as "wdpa_protected_area__iucn_cat",
      $"data_group.aze" as "is__alliance_for_zero_extinction_site",
      $"data_group.plantations" as "gfw_plantation__type",
      //      $"data_group.riverBasins" as "river_basin__name",
      //      $"data_group.ecozones" as "ecozone__name",
      //      $"data_group.urbanWatersheds" as "is__urban_water_intake",
      $"data_group.mangroves1996" as "is__mangroves_1996",
      $"data_group.mangroves2016" as "is__mangroves_2016",
      //      $"data_group.waterStress" as "baseline_water_stress__level",
      $"data_group.intactForestLandscapes" as "intact_forest_landscape__year",
      //      $"data_group.endemicBirdAreas" as "is__endemic_bird_area",
      $"data_group.tigerLandscapes" as "is__tiger_conservation_landscape",
      $"data_group.landmark" as "is__landmark",
      $"data_group.landRights" as "is__gfw_land_right",
      $"data_group.keyBiodiversityAreas" as "is__key_biodiversity_area",
      $"data_group.mining" as "is__gfw_mining",
      //      $"data_group.rspo" as "rspo_oil_palm__certification_status",
      $"data_group.peatlands" as "is__peat_land",
      $"data_group.oilPalm" as "is__gfw_oil_palm",
      $"data_group.idnForestMoratorium" as "is__idn_forest_moratorium",
      //      $"data_group.idnLandCover" as "idn_land_cover__class",
      //      $"data_group.mexProtectedAreas" as "is__mex_protected_areas",
      //      $"data_group.mexPaymentForEcosystemServices" as "is__mex_psa",
      //      $"data_group.mexForestZoning" as "mex_forest_zoning__zone",
      //      $"data_group.perProductionForest" as "is__per_permanent_production_forest",
      //      $"data_group.perProtectedAreas" as "is__per_protected_area",
      //      $"data_group.perForestConcessions" as "per_forest_concession__type",
      //      $"data_group.braBiomes" as "bra_biome__name",
      $"data_group.woodFiber" as "is__gfw_wood_fiber",
      $"data_group.resourceRights" as "is__gfw_resource_right",
      $"data_group.logging" as "is__gfw_logging",
      //      $"data_group.oilGas" as "is__gfw_oil_gas",
      $"data.treecoverExtent2000" as "treecover_extent_2000__ha",
      $"data.treecoverExtent2010" as "treecover_extent_2010__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalGainArea" as "treecover_gain_2000-2012__ha",
      $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
      $"data.totalCo2" as "aboveground_co2_stock_2000__Mg",
      //      $"data.totalMangroveBiomass" as "mangrove_aboveground_biomass_stock_2000__Mg",
      //      $"data.totalMangroveCo2" as "mangrove_aboveground_co2_stock_2000__Mg",
      $"data.treecoverLoss" as "treecover_loss__ha",
      $"data.biomassLoss" as "aboveground_biomass_loss__Mg",
      $"data.co2Emissions" as "aboveground_co2_emissions__Mg"
      //      $"data.mangroveBiomassLoss" as "mangrove_aboveground_biomass_loss__Mg",
      //      $"data.mangroveCo2Emissions" as "mangrove_aboveground_co2_emissions__Mg"
    )
  }
}
