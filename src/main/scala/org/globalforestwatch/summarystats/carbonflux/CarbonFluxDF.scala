package org.globalforestwatch.summarystats.carbonflux

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.{length, max, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CarbonFluxDF {

  val contextualLayers: List[String] = List(
    "is__flux_model_extent",
    "flux_removal_forest_type__name",
    "umd_tree_cover_density_2000__threshold",
    "is__umd_tree_cover_loss",
    "is__umd_tree_cover_gain",
    "is__mangrove_biomass_extent",
    "wri_google_tree_cover_loss_drivers__driver",
    "fao_ecozones_2000__class",
    "is__landmark_indigenous_and_community_lands",
    "wdpa_protected_areas__iucn_cat",
    "is__ifl_intact_forest_landscapes_2000",
    "gfw_plantation_flux_model__type",
    "is__intact_primary_forest",
    "is__gfw_peatlands",
    "gfw_forest_age_category__cat",
    "is__jpl_aboveground_biomass_extent",
    "usfs_fia_region__name",
    "brazil_biome__name",
    "mapbox_river_basin__name",
    "is__umd_regional_primary_forest_2001",
    "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
    "is__prodes_legal_Amazon_extent_2000",
    "is__tropical_latitude_extent",
    "is__tree_cover_loss_from_fires",
    "gfw_full_extent_gross_emissions_code__type",
    "is__gfw_pre_2000_plantations"
  )

  def unpackValues(df: DataFrame): DataFrame = {

    implicit val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"data_group.fluxModelExtent" as "is__flux_model_extent",
      $"data_group.removalForestType" as "flux_removal_forest_type__name",
      $"data_group.lossYear" as "umd_tree_cover_loss__year",
      $"data_group.threshold" as "umd_tree_cover_density_2000__threshold",
      $"data_group.isLoss" as "is__umd_tree_cover_loss",
      $"data_group.isGain" as "is__umd_tree_cover_gain",
      $"data_group.mangroveBiomassExtent" as "is__mangrove_biomass_extent",
      $"data_group.drivers" as "wri_google_tree_cover_loss_drivers__driver",
      $"data_group.faoEcozones2000" as "fao_ecozones_2000__class",
      $"data_group.landmark" as "is__landmark_indigenous_and_community_lands",
      $"data_group.wdpa" as "wdpa_protected_areas__iucn_cat",
      $"data_group.intactForestLandscapes2000" as "is__ifl_intact_forest_landscapes_2000",
      $"data_group.plantationsTypeFluxModel" as "gfw_plantation_flux_model__type",
      $"data_group.intactPrimaryForest" as "is__intact_primary_forest",
      $"data_group.peatlands" as "is__gfw_peatlands",
      $"data_group.forestAgeCategory" as "gfw_forest_age_category__cat",
      $"data_group.jplTropicsAbovegroundBiomassExtent2000" as "is__jpl_aboveground_biomass_extent",
      $"data_group.fiaRegionsUsExtent" as "usfs_fia_region__name",
      $"data_group.brazilBiomes" as "brazil_biome__name",
      $"data_group.riverBasins" as "mapbox_river_basin__name",
      $"data_group.primaryForest" as "is__umd_regional_primary_forest_2001",
      $"data_group.isLossLegalAmazon" as "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
      $"data_group.prodesLegalAmazonExtent2000" as "is__prodes_legal_Amazon_extent_2000",
      $"data_group.tropicLatitudeExtent" as "is__tropical_latitude_extent",
      $"data_group.treeCoverLossFromFires" as "is__tree_cover_loss_from_fires",
      $"data_group.grossEmissionsNodeCodes" as "gfw_full_extent_gross_emissions_code__type",
      $"data_group.plantationsPre2000" as "is__gfw_pre_2000_plantations",


      $"data.totalTreecoverLoss" as "umd_tree_cover_loss__ha",
      $"data.totalBiomassLoss" as "whrc_aboveground_biomass_loss__Mg_AGB",

      $"data.totalGrossEmissionsCo2eCo2OnlyBiomassSoil" as "gfw_full_extent_gross_emissions_CO2_only_biomass_soil__Mg_CO2",
      $"data.totalGrossEmissionsCo2eCh4BiomassSoil" as "gfw_full_extent_gross_emissions_CH4_biomass_soil__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eN2oBiomassSoil" as "gfw_full_extent_gross_emissions_N2O_biomass_soil__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eNonCo2BiomassSoil" as "gfw_full_extent_gross_emissions_non_CO2_biomass_soil__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eBiomassSoil" as "gfw_full_extent_gross_emissions_biomass_soil__Mg_CO2e",

      $"data.totalGrossEmissionsCo2eCo2OnlySoilOnly" as "gfw_full_extent_gross_emissions_CO2_only_soil_only__Mg_CO2",
      $"data.totalGrossEmissionsCo2eCh4SoilOnly" as "gfw_full_extent_gross_emissions_CH4_soil_only__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eN2oSoilOnly" as "gfw_full_extent_gross_emissions_N2O_soil_only__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eNonCo2SoilOnly" as "gfw_full_extent_gross_emissions_non_CO2_soil_only__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eSoilOnly" as "gfw_full_extent_gross_emissions_soil_only__Mg_CO2e",

      $"data.totalAgcEmisYear" as "gfw_aboveground_carbon_stock_in_emissions_year__Mg",
      $"data.totalBgcEmisYear" as "gfw_belowground_carbon_stock_in_emissions_year__Mg",
      $"data.totalDeadwoodCarbonEmisYear" as "gfw_deadwood_carbon_stock_in_emissions_year__Mg",
      $"data.totalLitterCarbonEmisYear" as "gfw_litter_carbon_stock_in_emissions_year__Mg",
      $"data.totalSoilCarbonEmisYear" as "gfw_soil_carbon_stock_in_emissions_year__Mg",
      $"data.totalCarbonEmisYear" as "gfw_total_carbon_stock_in_emissions_year__Mg",

      $"data.totalTreecoverExtent2000" as "umd_tree_cover_extent_2000__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalBiomass" as "whrc_aboveground_biomass_stock_2000__Mg",
      $"data.totalGrossAnnualAbovegroundRemovalsCarbon" as "gfw_full_extent_aboveground_annual_removals__Mg_C",
      $"data.totalGrossAnnualBelowgroundRemovalsCarbon" as "gfw_full_extent_belowground_annual_removals__Mg_C",
      $"data.totalGrossAnnualAboveBelowgroundRemovalsCarbon" as "gfw_full_extent_annual_removals__Mg_C",
      $"data.totalGrossCumulAbovegroundRemovalsCo2" as "gfw_full_extent_aboveground_gross_removals__Mg_CO2",
      $"data.totalGrossCumulBelowgroundRemovalsCo2" as "gfw_full_extent_belowground_gross_removals__Mg_CO2",
      $"data.totalGrossCumulAboveBelowgroundRemovalsCo2" as "gfw_full_extent_gross_removals__Mg_CO2",
      $"data.totalNetFluxCo2" as "gfw_full_extent_net_flux__Mg_CO2e",
      $"data.totalAgc2000" as "gfw_aboveground_carbon_stock_2000__Mg",
      $"data.totalBgc2000" as "gfw_belowground_carbon_stock_2000__Mg",
      $"data.totalDeadwoodCarbon2000" as "gfw_deadwood_carbon_stock_2000__Mg",
      $"data.totalLitterCarbon2000" as "gfw_litter_carbon_stock_2000__Mg",
      $"data.totalSoilCarbon2000" as "gfw_soil_carbon_stock_2000__Mg",
      $"data.totalCarbon2000" as "gfw_total_carbon_stock_2000__Mg",
      $"data.totalJplTropicsAbovegroundBiomassDensity2000" as "jpl_tropics_aboveground_biomass_2000__Mg",
      $"data.totalTreecoverLossLegalAmazon" as "legal_amazon_umd_tree_cover_loss__ha",
      $"data.totalVarianceAnnualAbovegroundRemovalsCarbon" as "gfw_total_variance_annual_aboveground_carbon_removals__Mg^2_ha^2_yr^2",
      $"data.totalVarianceAnnualAbovegroundRemovalsCarbonCount" as "gfw_pixel_count_variance_annual_aboveground_carbon_removals__pixels",
      $"data.totalVarianceSoilCarbonEmisYear" as "gfw_total_variance_soil_carbon_2000_in_emissions_year__Mg^2_ha^2",
      $"data.totalVarianceSoilCarbonEmisYearCount" as "gfw_pixel_count_variance_soil_carbon_2000_in_emissions_year__pixels",
      $"data.totalFluxModelExtentArea" as "gfw_flux_model_extent__ha"
    )
  }

  def aggSummary(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
      .agg(
        sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
        sum("area__ha") as "area__ha",
        sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
        sum("gfw_full_extent_aboveground_annual_removals__Mg_C") as "gfw_full_extent_aboveground_annual_removals__Mg_C",
        sum("gfw_full_extent_belowground_annual_removals__Mg_C") as "gfw_full_extent_belowground_annual_removals__Mg_C",
        sum("gfw_full_extent_annual_removals__Mg_C") as "gfw_full_extent_annual_removals__Mg_C",
        sum("gfw_full_extent_aboveground_gross_removals__Mg_CO2") as "gfw_full_extent_aboveground_gross_removals__Mg_CO2",
        sum("gfw_full_extent_belowground_gross_removals__Mg_CO2") as "gfw_full_extent_belowground_gross_removals__Mg_CO2",
        sum("gfw_full_extent_gross_removals__Mg_CO2") as "gfw_full_extent_gross_removals__Mg_CO2",
        sum("gfw_full_extent_net_flux__Mg_CO2e") as "gfw_full_extent_net_flux__Mg_CO2e",
        sum("gfw_aboveground_carbon_stock_2000__Mg") as "gfw_aboveground_carbon_stock_2000__Mg",
        sum("gfw_belowground_carbon_stock_2000__Mg") as "gfw_belowground_carbon_stock_2000__Mg",
        sum("gfw_deadwood_carbon_stock_2000__Mg") as "gfw_deadwood_carbon_stock_2000__Mg",
        sum("gfw_litter_carbon_stock_2000__Mg") as "gfw_litter_carbon_stock_2000__Mg",
        sum("gfw_soil_carbon_stock_2000__Mg") as "gfw_soil_carbon_stock_2000__Mg",
        sum("gfw_total_carbon_stock_2000__Mg") as "gfw_total_carbon_stock_2000__Mg",
        sum("umd_tree_cover_loss__ha") as "umd_tree_cover_loss__ha",
        sum("whrc_aboveground_biomass_loss__Mg_AGB") as "whrc_aboveground_biomass_loss__Mg_AGB",
        sum("gfw_full_extent_gross_emissions_CO2_only_biomass_soil__Mg_CO2") as "gfw_full_extent_gross_emissions_CO2_only_biomass_soil__Mg_CO2",
        sum("gfw_full_extent_gross_emissions_CH4_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_CH4_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_N2O_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_N2O_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_non_CO2_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_non_CO2_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_CO2_only_soil_only__Mg_CO2") as "gfw_full_extent_gross_emissions_CO2_only_soil_only__Mg_CO2",
        sum("gfw_full_extent_gross_emissions_CH4_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_CH4_soil_only__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_N2O_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_N2O_soil_only__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_non_CO2_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_non_CO2_soil_only__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_soil_only__Mg_CO2e",

        sum("jpl_tropics_aboveground_biomass_2000__Mg") as "jpl_tropics_aboveground_biomass_2000__Mg",
        sum("legal_amazon_umd_tree_cover_loss__ha") as "legal_amazon_umd_tree_cover_loss__ha",
        sum("gfw_total_variance_annual_aboveground_carbon_removals__Mg^2_ha^2_yr^2") as "gfw_total_variance_annual_aboveground_carbon_removals__Mg^2_ha^2_yr^2",
        sum("gfw_pixel_count_variance_annual_aboveground_carbon_removals__pixels") as "gfw_pixel_count_variance_annual_aboveground_carbon_removals__pixels",
        sum("gfw_total_variance_soil_carbon_2000_in_emissions_year__Mg^2_ha^2") as "gfw_total_variance_soil_carbon_2000_in_emissions_year__Mg^2_ha^2",
        sum("gfw_pixel_count_variance_soil_carbon_2000_in_emissions_year__pixels") as "gfw_pixel_count_variance_soil_carbon_2000_in_emissions_year__pixels",
        sum("gfw_flux_model_extent__ha") as "gfw_flux_model_extent__ha"
      )
  }

  def aggSummary2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
      .agg(
        sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
        sum("area__ha") as "area__ha",
        sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
        sum("gfw_full_extent_aboveground_annual_removals__Mg_C") as "gfw_full_extent_aboveground_annual_removals__Mg_C",
        sum("gfw_full_extent_belowground_annual_removals__Mg_C") as "gfw_full_extent_belowground_annual_removals__Mg_C",
        sum("gfw_full_extent_annual_removals__Mg_C") as "gfw_full_extent_annual_removals__Mg_C",
        sum("gfw_full_extent_aboveground_gross_removals__Mg_CO2") as "gfw_full_extent_aboveground_gross_removals__Mg_CO2",
        sum("gfw_full_extent_belowground_gross_removals__Mg_CO2") as "gfw_full_extent_belowground_gross_removals__Mg_CO2",
        sum("gfw_full_extent_gross_removals__Mg_CO2") as "gfw_full_extent_gross_removals__Mg_CO2",
        sum("gfw_full_extent_net_flux__Mg_CO2e") as "gfw_full_extent_net_flux__Mg_CO2e",
        sum("gfw_aboveground_carbon_stock_2000__Mg") as "gfw_aboveground_carbon_stock_2000__Mg",
        sum("gfw_belowground_carbon_stock_2000__Mg") as "gfw_belowground_carbon_stock_2000__Mg",
        sum("gfw_deadwood_carbon_stock_2000__Mg") as "gfw_deadwood_carbon_stock_2000__Mg",
        sum("gfw_litter_carbon_stock_2000__Mg") as "gfw_litter_carbon_stock_2000__Mg",
        sum("gfw_soil_carbon_stock_2000__Mg") as "gfw_soil_carbon_stock_2000__Mg",
        sum("gfw_total_carbon_stock_2000__Mg") as "gfw_total_carbon_stock_2000__Mg",
        sum("umd_tree_cover_loss__ha") as "umd_tree_cover_loss__ha",
        sum("whrc_aboveground_biomass_loss__Mg_AGB") as "whrc_aboveground_biomass_loss__Mg_AGB",
        sum("gfw_full_extent_gross_emissions_CO2_only_biomass_soil__Mg_CO2") as "gfw_full_extent_gross_emissions_CO2_only_biomass_soil__Mg_CO2",
        sum("gfw_full_extent_gross_emissions_CH4_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_CH4_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_N2O_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_N2O_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_non_CO2_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_non_CO2_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_CO2_only_soil_only__Mg_CO2") as "gfw_full_extent_gross_emissions_CO2_only_soil_only__Mg_CO2",
        sum("gfw_full_extent_gross_emissions_CH4_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_CH4_soil_only__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_N2O_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_N2O_soil_only__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_non_CO2_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_non_CO2_soil_only__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_soil_only__Mg_CO2e",
        sum("jpl_tropics_aboveground_biomass_2000__Mg") as "jpl_tropics_aboveground_biomass_2000__Mg",
        sum("legal_amazon_umd_tree_cover_loss__ha") as "legal_amazon_umd_tree_cover_loss__ha",
        sum("gfw_total_variance_annual_aboveground_carbon_removals__Mg^2_ha^2_yr^2") as "gfw_total_variance_annual_aboveground_carbon_removals__Mg^2_ha^2_yr^2",
        sum("gfw_pixel_count_variance_annual_aboveground_carbon_removals__pixels") as "gfw_pixel_count_variance_annual_aboveground_carbon_removals__pixels",
        sum("gfw_total_variance_soil_carbon_2000_in_emissions_year__Mg^2_ha^2") as "gfw_total_variance_soil_carbon_2000_in_emissions_year__Mg^2_ha^2",
        sum("gfw_pixel_count_variance_soil_carbon_2000_in_emissions_year__pixels") as "gfw_pixel_count_variance_soil_carbon_2000_in_emissions_year__pixels",
        sum("gfw_flux_model_extent__ha") as "gfw_flux_model_extent__ha"

      )
  }

  def aggChange(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(
        groupByCols.head,
      groupByCols.tail ::: List("umd_tree_cover_loss__year") ::: contextualLayers: _*
      )
      .agg(
        sum("umd_tree_cover_loss__ha") as "umd_tree_cover_loss__ha",
        sum("whrc_aboveground_biomass_loss__Mg_AGB") as "whrc_aboveground_biomass_loss__Mg_AGB",
        sum("gfw_full_extent_gross_emissions_CO2_only_biomass_soil__Mg_CO2") as "gfw_full_extent_gross_emissions_CO2_only_biomass_soil__Mg_CO2",
        sum("gfw_full_extent_gross_emissions_CH4_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_CH4_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_N2O_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_N2O_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_non_CO2_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_non_CO2_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_biomass_soil__Mg_CO2e") as "gfw_full_extent_gross_emissions_biomass_soil__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_CO2_only_soil_only__Mg_CO2") as "gfw_full_extent_gross_emissions_CO2_only_soil_only__Mg_CO2",
        sum("gfw_full_extent_gross_emissions_CH4_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_CH4_soil_only__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_N2O_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_N2O_soil_only__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_non_CO2_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_non_CO2_soil_only__Mg_CO2e",
        sum("gfw_full_extent_gross_emissions_soil_only__Mg_CO2e") as "gfw_full_extent_gross_emissions_soil_only__Mg_CO2e",
        sum("gfw_aboveground_carbon_stock_in_emissions_year__Mg") as "gfw_aboveground_carbon_stock_in_emissions_year__Mg",
        sum("gfw_belowground_carbon_stock_in_emissions_year__Mg") as "gfw_belowground_carbon_stock_in_emissions_year__Mg",
        sum("gfw_deadwood_carbon_stock_in_emissions_year__Mg") as "gfw_deadwood_carbon_stock_in_emissions_year__Mg",
        sum("gfw_litter_carbon_stock_in_emissions_year__Mg") as "gfw_litter_carbon_stock_in_emissions_year__Mg",
        sum("gfw_soil_carbon_stock_in_emissions_year__Mg") as "gfw_soil_carbon_stock_in_emissions_year__Mg",
        sum("gfw_total_carbon_stock_in_emissions_year__Mg") as "gfw_total_carbon_stock_in_emissions_year__Mg",
        sum("gfw_full_extent_gross_removals__Mg_CO2") as "gfw_full_extent_gross_removals__Mg_CO2"
      )
  }

  def whitelist(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        max($"is__flux_model_extent") as "is__flux_model_extent",
        max(length($"flux_removal_forest_type__name")).cast("boolean") as "flux_removal_forest_type__name",
        max($"is__umd_tree_cover_loss") as "is__umd_tree_cover_loss",
        max($"is__umd_tree_cover_gain") as "is__umd_tree_cover_gain",
        max($"is__mangrove_biomass_extent") as "is__mangrove_biomass_extent",
        max(length($"wri_google_tree_cover_loss_drivers__driver")).cast("boolean") as "wri_google_tree_cover_loss_drivers__driver",
        max(length($"fao_ecozones_2000__class")).cast("boolean") as "fao_ecozones_2000__class",
        max($"is__landmark_indigenous_and_community_lands") as "is__landmark_indigenous_and_community_lands",
        max(length($"wdpa_protected_areas__iucn_cat")).cast("boolean") as "wdpa_protected_areas__iucn_cat",
        max($"is__ifl_intact_forest_landscapes_2000") as "is__ifl_intact_forest_landscapes_2000",
        max(length($"gfw_plantation_flux_model__type")).cast("boolean") as "gfw_plantation_flux_model__type",
        max($"is__intact_primary_forest") as "is__intact_primary_forest",
        max($"is__gfw_peatlands") as "is__gfw_peatlands",
        max(length($"gfw_forest_age_category__cat")).cast("boolean") as "gfw_forest_age_category__cat",
        max($"is__jpl_aboveground_biomass_extent") as "is__jpl_aboveground_biomass_extent",
        max(length($"usfs_fia_region__name")).cast("boolean") as "usfs_fia_region__name",
        max(length($"brazil_biome__name")).cast("boolean") as "brazil_biome__name",
        max(length($"mapbox_river_basin__name")).cast("boolean") as "mapbox_river_basin__name",
        max($"is__umd_regional_primary_forest_2001") as "is__umd_regional_primary_forest_2001",
        max($"is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019") as "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
        max($"is__prodes_legal_Amazon_extent_2000") as "is__prodes_legal_Amazon_extent_2000",
        max($"is__tropical_latitude_extent") as "is__tropical_latitude_extent",
        max($"is__tree_cover_loss_from_fires") as "is__tree_cover_loss_from_fires",
        max(length($"gfw_full_extent_gross_emissions_code__type")).cast("boolean") as "gfw_full_extent_gross_emissions_code__type",
        max($"is__gfw_pre_2000_plantations") as "is__gfw_pre_2000_plantations"
      )
  }

  def whitelist2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        max($"is__flux_model_extent") as "is__flux_model_extent",
        max($"flux_removal_forest_type__name") as "flux_removal_forest_type__name",
        max($"is__umd_tree_cover_loss") as "is__umd_tree_cover_loss",
        max($"is__umd_tree_cover_gain") as "is__umd_tree_cover_gain",
        max($"is__mangrove_biomass_extent") as "is__mangrove_biomass_extent",
        max($"wri_google_tree_cover_loss_drivers__driver") as "wri_google_tree_cover_loss_drivers__driver",
        max($"fao_ecozones_2000__class") as "fao_ecozones_2000__class",
        max($"is__landmark_indigenous_and_community_lands") as "is__landmark_indigenous_and_community_lands",
        max($"wdpa_protected_areas__iucn_cat") as "wdpa_protected_areas__iucn_cat",
        max($"is__ifl_intact_forest_landscapes_2000") as "is__ifl_intact_forest_landscapes_2000",
        max($"gfw_plantation_flux_model__type") as "gfw_plantation_flux_model__type",
        max($"is__intact_primary_forest") as "is__intact_primary_forest",
        max($"is__gfw_peatlands") as "is__gfw_peatlands",
        max($"gfw_forest_age_category__cat") as "gfw_forest_age_category__cat",
        max($"is__jpl_aboveground_biomass_extent") as "is__jpl_aboveground_biomass_extent",
        max($"usfs_fia_region__name") as "usfs_fia_region__name",
        max($"brazil_biome__name") as "brazil_biome__name",
        max($"mapbox_river_basin__name") as "mapbox_river_basin__name",
        max($"is__umd_regional_primary_forest_2001") as "is__umd_regional_primary_forest_2001",
        max($"is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019") as "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
        max($"is__prodes_legal_Amazon_extent_2000") as "is__prodes_legal_Amazon_extent_2000",
        max($"is__tropical_latitude_extent") as "is__tropical_latitude_extent",
        max($"is__tree_cover_loss_from_fires") as "is__tree_cover_loss_from_fires",
        max($"gfw_full_extent_gross_emissions_code__type") as "gfw_full_extent_gross_emissions_code__type",
        max($"is__gfw_pre_2000_plantations") as "is__gfw_pre_2000_plantations"
      )
  }
}
