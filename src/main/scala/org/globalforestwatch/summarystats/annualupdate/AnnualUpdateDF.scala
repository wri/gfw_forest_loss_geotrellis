package org.globalforestwatch.summarystats.annualupdate

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

object AnnualUpdateDF {

  val contextualLayers = List(
    "umd_tree_cover_density__threshold",
    "tsc_tree_cover_loss_drivers__type",
    "esa_land_cover_2015__class",
    "is__umd_regional_primary_forest_2001",
    "is__idn_primary_forest",
    "aqueduct_erosion_risk__level",
    "is__birdlife_biodiversity_significance_top_10_perc",
    "is__birdlife_biodiversity_intactness_top_10_perc",
    "wdpa_protected_area__iucn_cat",
    "is__birdlife_alliance_for_zero_extinction_site",
    "gfw_plantation__type",
    "mapbox_river_basin__name",
    "wwf_eco_region__name",
    "is__tnc_urban_water_intake",
    "is__gmw_mangroves_1996",
    "is__gmw_mangroves_2016",
    "aqueduct_baseline_water_stress__level",
    "ifl_intact_forest_landscape__year",
    "is__birdlife_endemic_bird_area",
    "is__gfw_tiger_landscape",
    "is__landmark_land_right",
    "is__gfw_land_right",
    "is__birdlife_key_biodiversity_area",
    "is__gfw_mining",
    "rspo_oil_palm__certification_status",
    "is__peatland",
    "is__gfw_oil_palm",
    "is__idn_forest_moratorium",
    "idn_land_cover__class",
    "is__mex_protected_area",
    "is__mex_payment_ecosystem_service",
    "mex_forest_zoning__zone",
    "is__per_permanent_production_forest",
    "is__per_protected_area",
    "per_forest_concession__type",
    "bra_biome__name",
    "is__gfw_wood_fiber",
    "is__gfw_resource_right",
    "is__gfw_managed_forest",
    "is__gfw_oil_gas"
  )

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"data_group.lossYear" as "umd_tree_cover_loss__year",
      $"data_group.threshold" as "umd_tree_cover_density__threshold",
      $"data_group.drivers" as "tsc_tree_cover_loss_drivers__type",
      $"data_group.globalLandCover" as "esa_land_cover_2015__class",
      $"data_group.primaryForest" as "is__umd_regional_primary_forest_2001",
      $"data_group.idnPrimaryForest" as "is__idn_primary_forest",
      $"data_group.erosion" as "aqueduct_erosion_risk__level",
      $"data_group.biodiversitySignificance" as "is__birdlife_biodiversity_significance_top_10_perc",
      $"data_group.biodiversityIntactness" as "is__birdlife_biodiversity_intactness_top_10_perc",
      $"data_group.wdpa" as "wdpa_protected_area__iucn_cat",
      $"data_group.aze" as "is__birdlife_alliance_for_zero_extinction_site",
      $"data_group.plantations" as "gfw_plantation__type",
      $"data_group.riverBasins" as "mapbox_river_basin__name",
      $"data_group.ecozones" as "wwf_eco_region__name",
      $"data_group.urbanWatersheds" as "is__tnc_urban_water_intake",
      $"data_group.mangroves1996" as "is__gmw_mangroves_1996",
      $"data_group.mangroves2016" as "is__gmw_mangroves_2016",
      $"data_group.waterStress" as "aqueduct_baseline_water_stress__level",
      $"data_group.intactForestLandscapes" as "ifl_intact_forest_landscape__year",
      $"data_group.endemicBirdAreas" as "is__birdlife_endemic_bird_area",
      $"data_group.tigerLandscapes" as "is__gfw_tiger_landscape",
      $"data_group.landmark" as "is__landmark_land_right",
      $"data_group.landRights" as "is__gfw_land_right",
      $"data_group.keyBiodiversityAreas" as "is__birdlife_key_biodiversity_area",
      $"data_group.mining" as "is__gfw_mining",
      $"data_group.rspo" as "rspo_oil_palm__certification_status",
      $"data_group.peatlands" as "is__peatland",
      $"data_group.oilPalm" as "is__gfw_oil_palm",
      $"data_group.idnForestMoratorium" as "is__idn_forest_moratorium",
      $"data_group.idnLandCover" as "idn_land_cover__class",
      $"data_group.mexProtectedAreas" as "is__mex_protected_area",
      $"data_group.mexPaymentForEcosystemServices" as "is__mex_payment_ecosystem_service",
      $"data_group.mexForestZoning" as "mex_forest_zoning__zone",
      $"data_group.perProductionForest" as "is__per_permanent_production_forest",
      $"data_group.perProtectedAreas" as "is__per_protected_area",
      $"data_group.perForestConcessions" as "per_forest_concession__type",
      $"data_group.braBiomes" as "bra_biome__name",
      $"data_group.woodFiber" as "is__gfw_wood_fiber",
      $"data_group.resourceRights" as "is__gfw_resource_right",
      $"data_group.logging" as "is__gfw_managed_forest",
      $"data_group.oilGas" as "is__gfw_oil_gas",
      $"data.treecoverExtent2000" as "umd_tree_cover_extent_2000__ha",
      $"data.treecoverExtent2010" as "umd_tree_cover_extent_2010__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalGainArea" as "umd_tree_cover_gain_2000-2012__ha",
      $"data.totalBiomass" as "whrc_aboveground_biomass_stock_2000__Mg",
      $"data.totalCo2" as "whrc_aboveground_co2_stock_2000__Mg",
      $"data.totalMangroveBiomass" as "jpl_mangrove_aboveground_biomass_stock_2000__Mg",
      $"data.totalMangroveCo2" as "jpl_mangrove_aboveground_co2_stock_2000__Mg",
      $"data.treecoverLoss" as "umd_tree_cover_loss__ha",
      $"data.biomassLoss" as "whrc_aboveground_biomass_loss__Mg",
      $"data.co2Emissions" as "whrc_aboveground_co2_emissions__Mg",
      $"data.mangroveBiomassLoss" as "jpl_mangrove_aboveground_biomass_loss__Mg",
      $"data.mangroveCo2Emissions" as "jpl_mangrove_aboveground_co2_emissions__Mg"
    )
  }

  def aggSummary(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val cols = groupByCols ::: contextualLayers

    df.groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
        sum("umd_tree_cover_extent_2010__ha") as "umd_tree_cover_extent_2010__ha",
        sum("area__ha") as "area__ha",
        sum("umd_tree_cover_gain_2000-2012__ha") as "umd_tree_cover_gain_2000-2012__ha",
        sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
        sum("whrc_aboveground_co2_stock_2000__Mg") as "whrc_aboveground_co2_stock_2000__Mg",
        sum("jpl_mangrove_aboveground_biomass_stock_2000__Mg") as "jpl_mangrove_aboveground_biomass_stock_2000__Mg",
        sum("jpl_mangrove_aboveground_co2_stock_2000__Mg") as "jpl_mangrove_aboveground_co2_stock_2000__Mg",
        sum("umd_tree_cover_loss__ha") as "umd_tree_cover_loss_2001-2019__ha",
        sum("whrc_aboveground_biomass_loss__Mg") as "whrc_aboveground_biomass_loss_2001-2019__Mg",
        sum("whrc_aboveground_co2_emissions__Mg") as "whrc_aboveground_co2_emissions_2001-2019__Mg",
        sum("jpl_mangrove_aboveground_biomass_loss__Mg") as "jpl_mangrove_aboveground_biomass_loss_2001-2019__Mg",
        sum("jpl_mangrove_aboveground_co2_emissions__Mg") as "jpl_mangrove_aboveground_co2_emissions_2001-2019__Mg"
      )
  }

  def aggSummary2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val cols = groupByCols ::: contextualLayers

    df.groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
        sum("umd_tree_cover_extent_2010__ha") as "umd_tree_cover_extent_2010__ha",
        sum("area__ha") as "area__ha",
        sum("umd_tree_cover_gain_2000-2012__ha") as "umd_tree_cover_gain_2000-2012__ha",
        sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
        sum("whrc_aboveground_co2_stock_2000__Mg") as "whrc_aboveground_co2_stock_2000__Mg",
        sum("jpl_mangrove_aboveground_biomass_stock_2000__Mg") as "jpl_mangrove_aboveground_biomass_stock_2000__Mg",
        sum("jpl_mangrove_aboveground_co2_stock_2000__Mg") as "jpl_mangrove_aboveground_co2_stock_2000__Mg",
        sum("umd_tree_cover_loss_2001-2019__ha") as "umd_tree_cover_loss_2001-2019__ha",
        sum("whrc_aboveground_biomass_loss_2001-2019__Mg") as "whrc_aboveground_biomass_loss_2001-2019__Mg",
        sum("whrc_aboveground_co2_emissions_2001-2019__Mg") as "whrc_aboveground_co2_emissions_2001-2019__Mg",
        sum("jpl_mangrove_aboveground_biomass_loss_2001-2019__Mg") as "jpl_mangrove_aboveground_biomass_loss_2001-2019__Mg",
        sum("jpl_mangrove_aboveground_co2_emissions_2001-2019__Mg") as "jpl_mangrove_aboveground_co2_emissions_2001-2019__Mg"
      )
  }

  def aggChange(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val cols = groupByCols ::: List("umd_tree_cover_loss__year") ::: contextualLayers

    df.groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("umd_tree_cover_loss__ha") as "umd_tree_cover_loss__ha",
        sum("whrc_aboveground_biomass_loss__Mg") as "whrc_aboveground_biomass_loss__Mg",
        sum("whrc_aboveground_co2_emissions__Mg") as "whrc_aboveground_co2_emissions__Mg",
        sum("jpl_mangrove_aboveground_biomass_loss__Mg") as "jpl_mangrove_aboveground_biomass_loss__Mg",
        sum("jpl_mangrove_aboveground_co2_emissions__Mg") as "jpl_mangrove_aboveground_co2_emissions__Mg"
      )
  }
}
