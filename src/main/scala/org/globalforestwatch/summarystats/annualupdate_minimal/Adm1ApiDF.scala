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
      $"tcs_drivers",
        $"global_land_cover",
      $"regional_primary_forests",
      //      $"idn_primary_forests",
      //      $"erosion_risk",
      //      $"biodiversity_significance_top_10_perc",
      //      $"biodiversity_intactness_top_10_perc",
      $"wdpa_protected_areas",
      $"alliance_for_zero_extinction_sites",
      $"gfw_plantations",
      //      $"river_basins",
      //      $"ecozones",
      //      $"urban_water_intakes",
      $"mangroves_1996",
      $"mangroves_2016",
      //      $"baseline_water_stress",
      $"intact_forest_landscapes",
      //      $"endemic_bird_areas",
      $"tiger_conservation_landscapes",
        $"landmark",
      $"gfw_land_rights",
      $"key_biodiversity_areas",
      $"gfw_mining",
      //      $"rspo_oil_palm",
      $"peat_lands",
      $"gfw_oil_palm",
        $"idn_forest_moratorium",
      //      $"idn_land_cover",
      //      $"mex_protected_areas",
      //      $"mex_psa",
      //      $"mex_forest_zoning",
      //      $"per_permanent_production_forests",
      //      $"per_protected_areas",
      //      $"per_forest_concessions",
      //      $"bra_biomes",
      $"gfw_wood_fiber",
      $"gfw_resource_rights",
      $"gfw_logging"
      //      $"gfw_oil_gas",
      )
      .agg(
        sum("treecover_loss__ha") as "treecover_loss__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss__Mg",
        sum("co2_emissions__Mg") as "co2_emissions__Mg"
        //        sum("mangrove_aboveground_biomass_loss__Mg") as "mangrove_aboveground_biomass_loss__Mg",
        //        sum("mangrove_co2_emissions__Mg") as "mangrove_co2_emissions__Mg"
      )
  }

  def sumArea(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
      $"iso",
      $"adm1",
      $"threshold",
      $"tcs_drivers",
      $"global_land_cover",
      $"regional_primary_forests",
      //      $"idn_primary_forests",
      //      $"erosion_risk",
      //      $"biodiversity_significance_top_10_perc",
      //      $"biodiversity_intactness_top_10_perc",
      $"wdpa_protected_areas",
      $"alliance_for_zero_extinction_sites",
      $"gfw_plantations",
      //      $"river_basins",
      //      $"ecozones",
      //      $"urban_water_intakes",
      $"mangroves_1996",
      $"mangroves_2016",
      //      $"baseline_water_stress",
      $"intact_forest_landscapes",
      //      $"endemic_bird_areas",
      $"tiger_conservation_landscapes",
      $"landmark",
      $"gfw_land_rights",
      $"key_biodiversity_areas",
      $"gfw_mining",
      //      $"rspo_oil_palm",
      $"peat_lands",
      $"gfw_oil_palm",
      $"idn_forest_moratorium",
      //      $"idn_land_cover",
      //      $"mex_protected_areas",
      //      $"mex_psa",
      //      $"mex_forest_zoning",
      //      $"per_permanent_production_forests",
      //      $"per_protected_areas",
      //      $"per_forest_concessions",
      //      $"bra_biomes",
      $"gfw_wood_fiber",
      $"gfw_resource_rights",
      $"gfw_logging"
      //      $"gfw_oil_gas",
    )
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("treecover_extent_2010__ha") as "treecover_extent_2010__ha",
        sum("area__ha") as "area__ha",
        sum("treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("co2_stock__Mg") as "co2_stock__Mg"
        //        sum("mangrove_aboveground_biomass_stock__Mg") as "mangrove_aboveground_biomass_stock__Mg",
        //        sum("mangrove_co2_stock__Mg") as "mangrove_co2_stock__Mg"
      )
  }
}
