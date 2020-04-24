package org.globalforestwatch.summarystats.carbonflux

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.{length, max, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}


object CarbonFluxDF {

  val contextualLayers: List[String] = List(
    "umd_tree_cover_density__threshold",
    "is__umd_tree_cover_loss_2000-2015",
    "is__umd_tree_cover_gain_2000-2012",
    "is__gmw_mangrove_extent",
    "tsc_tree_cover_loss_drivers__type",
    "wwf_eco_region__name",
    "is__gfw_land_right",
    "wdpa_protected_area__iucn_cat",
    "ifl_intact_forest_landscape__year",
    "gfw_plantation__type",
    "is__intact_primary_forest",
    "is__peatland_flux",
    "gfw_forest_age_category__cat",
    "is__jpl_aboveground_biomass_extent",
    "usfs_fia_region__name"
  )


  def unpackValues(df: DataFrame): DataFrame = {

    implicit val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "dataGroup", "data"))

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"dataGroup.lossYear" as "umd_tree_cover_loss__year",
      $"dataGroup.threshold" as "umd_tree_cover_density__threshold",
      $"dataGroup.isGain" as "is__umd_tree_cover_gain_2000-2012",
      $"dataGroup.isLoss" as "is__umd_tree_cover_loss_2000-2015",
      $"dataGroup.mangroveBiomassExtent" as "is__gmw_mangrove_extent",
      $"dataGroup.drivers" as "tsc_tree_cover_loss_drivers__type",
      $"dataGroup.ecozones" as "wwf_eco_region__name",
      $"dataGroup.landRights" as "is__gfw_land_right",
      $"dataGroup.wdpa" as "wdpa_protected_area__iucn_cat",
      $"dataGroup.intactForestLandscapes" as "ifl_intact_forest_landscape__year",
      $"dataGroup.plantations" as "gfw_plantation__type",
      $"dataGroup.intactPrimaryForest" as "is__intact_primary_forest",
      $"dataGroup.peatlandsFlux" as "is__peatland_flux",
      $"dataGroup.forestAgeCategory" as "gfw_forest_age_category__cat",
      $"dataGroup.jplAGBextent" as "is__jpl_aboveground_biomass_extent",
      $"dataGroup.fiaRegionsUsExtent" as "usfs_fia_region__name",
      $"data.treecoverLoss" as "umd_tree_cover_loss__ha",
      $"data.biomassLoss" as "whrc_aboveground_biomass_loss__Mg",
      $"data.grossEmissionsCo2eCo2Only" as "gfw_gross_emissions_co2e_co2_only__Mg",
      $"data.grossEmissionsCo2eNoneCo2" as "gfw_gross_emissions_co2e_non_co2__Mg",
      $"data.grossEmissionsCo2e" as "gfw_gross_emissions_co2e_all_gases__Mg",
      $"data.agcEmisYear" as "gfw_aboveground_carbon_stock_in_emissions_year__Mg",
      $"data.bgcEmisYear" as "gfw_belowground_carbon_stock_in_emissions_year__Mg",
      $"data.deadwoodCarbonEmisYear" as "gfw_deadwood_carbon_stock_in_emissions_year__Mg",
      $"data.litterCarbonEmisYear" as "gfw_litter_carbon_stock_in_emissions_year__Mg",
      $"data.soilCarbonEmisYear" as "gfw_soil_carbon_stock_in_emissions_year__Mg",
      $"data.carbonEmisYear" as "gfw_total_carbon_stock_in_emissions_year__Mg",
      $"data.treecoverExtent2000" as "umd_tree_cover_extent_2000__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalBiomass" as "whrc_aboveground_biomass_stock_2000__Mg",
      $"data.totalGrossAnnualRemovalsCarbon" as "gfw_gross_annual_biomass_removals_2001-2015__Mg",
      $"data.totalGrossCumulRemovalsCarbon" as "gfw_gross_cumulative_co2_removals_2001-2015__Mg",
      $"data.totalNetFluxCo2" as "gfw_net_flux_co2_2001-2015__Mg",
      $"data.totalAgc2000" as "gfw_aboveground_carbon_stock_2000__Mg",
      $"data.totalBgc2000" as "gfw_belowground_carbon_stock_2000__Mg",
      $"data.totalDeadwoodCarbon2000" as "gfw_deadwood_carbon_stock_2000__Mg",
      $"data.totalLitterCarbon2000" as "gfw_litter_carbon_stock_2000__Mg",
      $"data.totalSoil2000" as "gfw_soil_carbon_stock_2000__Mg",
      $"data.totalCarbon2000" as "gfw_total_carbon_stock_2000__Mg"
    )
  }

  def aggSummary(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
      .agg(
        sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
        sum("area__ha") as "area__ha",
        sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
        sum("gfw_gross_annual_biomass_removals_2001-2015__Mg") as "gfw_gross_annual_biomass_removals_2001-2015__Mg",
        sum("gfw_gross_cumulative_co2_removals_2001-2015__Mg") as "gfw_gross_cumulative_co2_removals_2001-2015__Mg",
        sum("gfw_net_flux_co2_2001-2015__Mg") as "gfw_net_flux_co2_2001-2015__Mg",
        sum("gfw_aboveground_carbon_stock_2000__Mg") as "gfw_aboveground_carbon_stock_2000__Mg",
        sum("gfw_belowground_carbon_stock_2000__Mg") as "gfw_belowground_carbon_stock_2000__Mg",
        sum("gfw_deadwood_carbon_stock_2000__Mg") as "gfw_deadwood_carbon_stock_2000__Mg",
        sum("gfw_litter_carbon_stock_2000__Mg") as "gfw_litter_carbon_stock_2000__Mg",
        sum("gfw_soil_carbon_stock_2000__Mg") as "gfw_soil_carbon_stock_2000__Mg",
        sum("gfw_total_carbon_stock_2000__Mg") as "gfw_total_carbon_stock_2000__Mg",
        sum("umd_tree_cover_loss__ha") as "treecover_loss_2001-2015__ha",
        sum("whrc_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2001-2015__Mg",
        sum("gfw_gross_emissions_co2e_co2_only__Mg") as "gfw_gross_emissions_co2e_co2_only_2001-2015__Mg",
        sum("gfw_gross_emissions_co2e_non_co2__Mg") as "gfw_gross_emissions_co2e_non_co2_2001-2015__Mg",
        sum("gfw_gross_emissions_co2e_all_gases__Mg") as "gfw_gross_emissions_co2e_all_gases_2001-2015__Mg"
      )
  }

  def aggSummary2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
      .agg(
        sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
        sum("area__ha") as "area__ha",
        sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
        sum("gfw_gross_annual_biomass_removals_2001-2015__Mg") as "gfw_gross_annual_biomass_removals_2001-2015__Mg",
        sum("gfw_gross_cumulative_co2_removals_2001-2015__Mg") as "gfw_gross_cumulative_co2_removals_2001-2015__Mg",
        sum("gfw_net_flux_co2_2001-2015__Mg") as "gfw_net_flux_co2_2001-2015__Mg",
        sum("gfw_aboveground_carbon_stock_2000__Mg") as "gfw_aboveground_carbon_stock_2000__Mg",
        sum("gfw_belowground_carbon_stock_2000__Mg") as "gfw_belowground_carbon_stock_2000__Mg",
        sum("gfw_deadwood_carbon_stock_2000__Mg") as "gfw_deadwood_carbon_stock_2000__Mg",
        sum("gfw_litter_carbon_stock_2000__Mg") as "gfw_litter_carbon_stock_2000__Mg",
        sum("gfw_soil_carbon_stock_2000__Mg") as "gfw_soil_carbon_stock_2000__Mg",
        sum("gfw_total_carbon_stock_2000__Mg") as "gfw_total_carbon_stock_2000__Mg",
        sum("treecover_loss_2001-2015__ha") as "treecover_loss_2001-2015__ha",
        sum("aboveground_biomass_loss_2001-2015__Mg") as "aboveground_biomass_loss_2001-2015__Mg",
        sum("gfw_gross_emissions_co2e_co2_only_2001-2015__Mg") as "gfw_gross_emissions_co2e_co2_only_2001-2015__Mg",
        sum("gfw_gross_emissions_co2e_non_co2_2001-2015__Mg") as "gfw_gross_emissions_co2e_non_co2_2001-2015__Mg",
        sum("gfw_gross_emissions_co2e_all_gases_2001-2015__Mg") as "gfw_gross_emissions_co2e_all_gases_2001-2015__Mg"
      )
  }

  def aggChange(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(
        groupByCols.head,
      groupByCols.tail ::: List("umd_tree_cover_loss__year") ::: contextualLayers: _*
      )
      .agg(
        sum("umd_tree_cover_loss__ha") as "umd_tree_cover_loss__ha",
        sum("whrc_aboveground_biomass_loss__Mg") as "whrc_aboveground_biomass_loss__Mg",
        sum("gfw_gross_emissions_co2e_co2_only__Mg") as "gfw_gross_emissions_co2e_co2_only__Mg",
        sum("gfw_gross_emissions_co2e_non_co2__Mg") as "gfw_gross_emissions_co2e_non_co2__Mg",
        sum("gfw_gross_emissions_co2e_all_gases__Mg") as "gfw_gross_emissions_co2e_all_gases__Mg",
        sum("gfw_aboveground_carbon_stock_in_emissions_year__Mg") as "gfw_aboveground_carbon_stock_in_emissions_year__Mg",
        sum("gfw_belowground_carbon_stock_in_emissions_year__Mg") as "gfw_belowground_carbon_stock_in_emissions_year__Mg",
        sum("gfw_deadwood_carbon_stock_in_emissions_year__Mg") as "gfw_deadwood_carbon_stock_in_emissions_year__Mg",
        sum("gfw_litter_carbon_stock_in_emissions_year__Mg") as "gfw_litter_carbon_stock_in_emissions_year__Mg",
        sum("gfw_soil_carbon_stock_in_emissions_year__Mg") as "gfw_soil_carbon_stock_in_emissions_year__Mg",
        sum("gfw_total_carbon_stock_in_emissions_year__Mg") as "gfw_total_carbon_stock_in_emissions_year__Mg"
      )
  }

  def whitelist(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        max($"is__umd_tree_cover_loss_2000-2015") as "is__umd_tree_cover_loss_2000-2015",
        max($"is__umd_tree_cover_gain_2000-2012") as "is__umd_tree_cover_gain_2000-2012",
        max($"is__gmw_mangrove_extent") as "is__gmw_mangrove_extent",
        max(length($"tsc_tree_cover_loss_drivers__type")).cast("boolean") as "tsc_tree_cover_loss_drivers__type",
        max(length($"wwf_eco_region__name")).cast("boolean") as "wwf_eco_region__name",
        max($"is__gfw_land_right") as "is__gfw_land_right",
        max(length($"wdpa_protected_area__iucn_cat"))
          .cast("boolean") as "wdpa_protected_area__iucn_cat",
        max(length($"ifl_intact_forest_landscape__year"))
          .cast("boolean") as "ifl_intact_forest_landscape__year",
        max(length($"gfw_plantation__type"))
          .cast("boolean") as "gfw_plantation__type",
        max($"is__intact_primary_forest") as "is__intact_primary_forest",
        max($"is__peatland_flux") as "is__peatland_flux",
        max(length($"gfw_forest_age_category__cat"))
          .cast("boolean") as "gfw_forest_age_category__cat",
        max($"is__jpl_aboveground_biomass_extent") as "is__jpl_aboveground_biomass_extent",
        max(length($"usfs_fia_region__name"))
          .cast("boolean") as "usfs_fia_region__name"
      )
  }

  def whitelist2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        max($"is__umd_tree_cover_loss_2000-2015") as "is__umd_tree_cover_loss_2000-2015",
        max($"is__umd_tree_cover_gain_2000-2012") as "is__umd_tree_cover_gain_2000-2012",
        max($"is__gmw_mangrove_extent") as "is__gmw_mangrove_extent",
        max($"tsc_tree_cover_loss_drivers__type") as "tsc_tree_cover_loss_drivers__type",
        max($"wwf_eco_region__name") as "wwf_eco_region__name",
        max($"is__gfw_land_right") as "is__gfw_land_right",
        max($"wdpa_protected_area__iucn_cat") as "wdpa_protected_area__iucn_cat",
        max($"ifl_intact_forest_landscape__year") as "ifl_intact_forest_landscape__year",
        max($"gfw_plantation__type") as "gfw_plantation__type",
        max($"is__intact_primary_forest") as "is__intact_primary_forest",
        max($"is__peatland_flux") as "is__peatland_flux",
        max($"gfw_forest_age_category__cat") as "gfw_forest_age_category__cat",
        max($"is__jpl_aboveground_biomass_extent") as "is__jpl_aboveground_biomass_extent",
        max($"usfs_fia_region__name") as "usfs_fia_region__name"
      )
  }


}
