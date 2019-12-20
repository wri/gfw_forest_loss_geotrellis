package org.globalforestwatch.summarystats.annualupdate_minimal.dataframes

import org.apache.spark.sql.functions.{max, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Adm1ApiDF {
  def sumChange(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
      $"iso",
      $"adm1",
      $"treecover_loss__year",
      $"treecover_density__threshold",
      $"tcs_driver__type",
      $"global_land_cover__class",
      $"is__regional_primary_forest",
      //      $"is__idn_primary_forest",
      //      $"erosion_risk__level",
      //      $"is__biodiversity_significance_top_10_perc",
      //      $"is__biodiversity_intactness_top_10_perc",
      $"wdpa_protected_area__iucn_cat",
      $"is__alliance_for_zero_extinction_site",
      $"gfw_plantation__type",
      //      $"river_basin__name",
      //      $"ecozone__name",
      //      $"is__urban_water_intake",
      $"is__mangroves_1996",
      $"is__mangroves_2016",
      //      $"baseline_water_stress__level",
      $"intact_forest_landscape__year",
      //      $"is__endemic_bird_area",
      $"is__tiger_conservation_landscape",
      $"is__landmark",
      $"is__gfw_land_right",
      $"is__key_biodiversity_area",
      $"is__gfw_mining",
      //      $"rspo_oil_palm__certification_status",
      $"is__peat_land",
      $"is__gfw_oil_palm",
      $"is__idn_forest_moratorium",
      //      $"idn_land_cover__class",
      //      $"is__mex_protected_areas",
      //      $"is__mex_psa",
      //      $"mex_forest_zoning__zone",
      //      $"is__per_permanent_production_forest",
      //      $"is__per_protected_area",
      //      $"per_forest_concession__type",
      //      $"bra_biome__name",
      $"is__gfw_wood_fiber",
      $"is__gfw_resource_right",
      $"is__gfw_logging"
      //      $"is__gfw_oil_gas",
    )
      .agg(
        sum("treecover_loss__ha") as "treecover_loss__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss__Mg",
        sum("aboveground_co2_emissions__Mg") as "aboveground_co2_emissions__Mg"
        //        sum("mangrove_aboveground_biomass_loss__Mg") as "mangrove_aboveground_biomass_loss__Mg",
        //        sum("mangrove_aboveground_co2_emissions__Mg") as "mangrove_aboveground_co2_emissions__Mg"
      )
  }

  def sumArea(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy(
      $"iso",
      $"adm1",
      $"treecover_density__threshold",
      $"tcs_driver__type",
      $"global_land_cover__class",
      $"is__regional_primary_forest",
      //      $"is__idn_primary_forest",
      //      $"erosion_risk__level",
      //      $"is__biodiversity_significance_top_10_perc",
      //      $"is__biodiversity_intactness_top_10_perc",
      $"wdpa_protected_area__iucn_cat",
      $"is__alliance_for_zero_extinction_site",
      $"gfw_plantation__type",
      //      $"river_basin__name",
      //      $"ecozone__name",
      //      $"is__urban_water_intake",
      $"is__mangroves_1996",
      $"is__mangroves_2016",
      //      $"baseline_water_stress__level",
      $"intact_forest_landscape__year",
      //      $"is__endemic_bird_area",
      $"is__tiger_conservation_landscape",
      $"is__landmark",
      $"is__gfw_land_right",
      $"is__key_biodiversity_area",
      $"is__gfw_mining",
      //      $"rspo_oil_palm__certification_status",
      $"is__peat_land",
      $"is__gfw_oil_palm",
      $"is__idn_forest_moratorium",
      //      $"idn_land_cover__class",
      //      $"is__mex_protected_areas",
      //      $"is__mex_psa",
      //      $"mex_forest_zoning__zone",
      //      $"is__per_permanent_production_forest",
      //      $"is__per_protected_area",
      //      $"per_forest_concession__type",
      //      $"bra_biome__name",
      $"is__gfw_wood_fiber",
      $"is__gfw_resource_right",
      $"is__gfw_logging"
      //      $"is__gfw_oil_gas",
    )
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("treecover_extent_2010__ha") as "treecover_extent_2010__ha",
        sum("area__ha") as "area__ha",
        sum("treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("aboveground_co2_stock_2000__Mg") as "aboveground_co2_stock_2000__Mg",
        //        sum("mangrove_aboveground_biomass_stock_2000__Mg") as "mangrove_aboveground_biomass_stock_2000__Mg",
        //        sum("mangrove_aboveground_co2_stock_2000__Mg") as "mangrove_aboveground_co2_stock_2000__Mg",
        sum("treecover_loss_2001-2018__ha") as "treecover_loss_2001-2018__ha",
        sum("aboveground_biomass_loss_2001-2018__Mg") as "aboveground_biomass_loss_2001-2018__Mg",
        sum("aboveground_co2_emissions_2001-2018__Mg") as "aboveground_co2_emissions_2001-2018__Mg"
        //        sum("mangrove_aboveground_biomass_loss_2001-2018__Mg") as "mangrove_aboveground_biomass_loss_2001-2018__Mg",
        //        sum("mangrove_co2_emissions_2001-2018__Mg") as "mangrove_co2_emissions_2001-2018__Mg"
      )
  }

  def whitelist(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy($"iso", $"adm1")
      .agg(
        max($"tcs_driver__type") as "tcs_driver__type",
        max($"global_land_cover__class") as "global_land_cover__class",
        max($"is__regional_primary_forest") as "is__regional_primary_forest",
        //      $"is__idn_primary_forest",
        //      $"erosion_risk__level",
        //      $"is__biodiversity_significance_top_10_perc",
        //      $"is__biodiversity_intactness_top_10_perc",
        max($"wdpa_protected_area__iucn_cat") as "wdpa_protected_area__iucn_cat",
        max($"is__alliance_for_zero_extinction_site") as "is__alliance_for_zero_extinction_site",
        max($"gfw_plantation__type") as "gfw_plantation__type",
        //      $"river_basin__name",
        //      $"ecozone__name",
        //      $"is__urban_water_intake",
        max($"is__mangroves_1996") as "is__mangroves_1996",
        max($"is__mangroves_2016") as "is__mangroves_2016",
        //      $"baseline_water_stress__level",
        max($"intact_forest_landscape__year") as "intact_forest_landscape__year",
        //      $"is__endemic_bird_area",
        max($"is__tiger_conservation_landscape") as "is__tiger_conservation_landscape",
        max($"is__landmark") as "is__landmark",
        max($"is__gfw_land_right") as "is__gfw_land_right",
        max($"is__key_biodiversity_area") as "is__key_biodiversity_area",
        max($"is__gfw_mining") as "is__gfw_mining",
        //      $"rspo_oil_palm__certification_status",
        max($"is__peat_land") as "is__peat_land",
        max($"is__gfw_oil_palm") as "is__gfw_oil_palm",
        max($"is__idn_forest_moratorium") as "is__idn_forest_moratorium",
        //      $"idn_land_cover__class",
        //      $"is__mex_protected_areas",
        //      $"is__mex_psa",
        //      $"mex_forest_zoning__zone",
        //      $"is__per_permanent_production_forest",
        //      $"is__per_protected_area",
        //      $"per_forest_concession__type",
        //      $"bra_biome__name",
        max($"is__gfw_wood_fiber") as "is__gfw_wood_fiber",
        max($"is__gfw_resource_right") as "is__gfw_resource_right",
        max($"is__gfw_logging") as "is__gfw_logging"
        //      $"is__gfw_oil_gas",
      )
  }
}
