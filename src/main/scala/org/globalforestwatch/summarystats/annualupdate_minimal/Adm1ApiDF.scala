package org.globalforestwatch.summarystats.annualupdate_minimal

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm1ApiDF {
  def sumChange(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
        $"iso",
        $"adm1",
      $"loss_year",
        $"threshold",
        $"tcs",
        $"global_land_cover",
        $"primary_forest",
      //      $"idn_primary_forest",
      //      $"erosion",
      //      $"biodiversity_significance",
      //      $"biodiversity_intactness",
        $"wdpa",
        $"aze",
        $"plantations",
      //      $"river_basin",
      //      $"ecozone",
      //      $"urban_watershed",
      $"mangroves_1996",
      $"mangroves_2016",
      //      $"water_stress",
        $"ifl",
      //      $"endemic_bird_area",
        $"tiger_cl",
        $"landmark",
        $"land_right",
        $"kba",
        $"mining",
      //      $"rspo",
      $"idn_mys_peatlands",
        $"oil_palm",
        $"idn_forest_moratorium",
      //      $"idn_land_cover",
      //      $"mex_protected_areas",
      //      $"mex_pes",
      //      $"mex_forest_zoning",
      //      $"per_production_forest",
      //      $"per_protected_area",
      //      $"per_forest_concession",
      //      $"bra_biomes",
        $"wood_fiber",
        $"resource_right",
      $"managed_forests"
      //      $"oil_gas",
      )
      .agg(
        sum("area_loss_ha") as "area_loss_ha",
        sum("biomass_loss_Mt") as "biomass_loss_Mt",
        sum("co2_emissions_Mt") as "co2_emissions_Mt"
        //        sum("mangrove_biomass_loss_Mt") as "mangrove_biomass_loss_Mt",
        //        sum("mangrove_co2_emissions_Mt") as "mangrove_co2_emissions_Mt"
      )
  }

  def sumArea(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
      $"iso",
      $"adm1",
      $"threshold",
      $"tcs",
      $"global_land_cover",
      $"primary_forest",
      //      $"idn_primary_forest",
      //      $"erosion",
      //      $"biodiversity_significance",
      //      $"biodiversity_intactness",
      $"wdpa",
      $"aze",
      $"plantations",
      //      $"river_basin",
      //      $"ecozone",
      //      $"urban_watershed",
      $"mangroves_1996",
      $"mangroves_2016",
      //      $"water_stress",
      $"ifl",
      //      $"endemic_bird_area",
      $"tiger_cl",
      $"landmark",
      $"land_right",
      $"kba",
      $"mining",
      //      $"rspo",
      $"idn_mys_peatlands",
      $"oil_palm",
      $"idn_forest_moratorium",
      //      $"idn_land_cover",
      //      $"mex_protected_areas",
      //      $"mex_pes",
      //      $"mex_forest_zoning",
      //      $"per_production_forest",
      //      $"per_protected_area",
      //      $"per_forest_concession",
      //      $"bra_biomes",
      $"wood_fiber",
      $"resource_right",
      $"managed_forests"
      //      $"oil_gas",
    )
      .agg(
        sum("treecover_extent_2000_ha") as "treecover_extent_2000_ha",
        sum("treecover_extent_2010_ha") as "treecover_extent_2010_ha",
        sum("total_area_ha") as "total_area_ha",
        sum("total_gain_2000-2012_ha") as "total_gain_2000-2012_ha",
        sum("total_biomass_stock_2000_Mt") as "total_biomass_stock_2000_Mt",
        sum("weighted_biomass_2000_Mt_ha-1") as "weighted_biomass_2000_Mt_ha-1",
        sum("total_co2_stock_Mt") as "total_co2_stock_Mt"
        //        sum("total_mangrove_biomass_stock_Mt") as "total_mangrove_biomass_stock_Mt",
        //        sum("weighted_mangrove_biomass_Mt_ha-1") as "weighted_mangrove_biomass_Mt_ha-1",
        //        sum("total_mangrove_co2_stock_Mt") as "total_mangrove_co2_stock_Mt"
      )
  }
}
