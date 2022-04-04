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
    "is__gmw_mangroves_extent",
    "tsc_tree_cover_loss_drivers__driver",
    "fao_ecozones__class",
    "is__landmark_indigenous_and_community_lands",
    "wdpa_protected_areas__iucn_cat",
    "ifl_intact_forest_landscapes__year",
    "gfw_plantation_flux_model__type",
    "is__intact_primary_forest",
    "is__peatlands_extent_flux_model",
    "gfw_forest_age_category__cat",
    "is__jpl_aboveground_biomass_extent",
    "usfs_fia_region__name",
    "bra_biome__name",
    "mapbox_river_basin__name",
    "is__umd_regional_primary_forest_2001",
    "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
    "is__prodes_legal_Amazon_extent_2000",
    "is__tropical_latitude_extent",
    "is__gfw_burn_loss_2001-2021",
    "gfw_gross_emissions_code__type"
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
      $"data_group.mangroveBiomassExtent" as "is__gmw_mangroves_extent",
      $"data_group.drivers" as "tsc_tree_cover_loss_drivers__driver",
      $"data_group.faoEcozones" as "fao_ecozones__class",
      $"data_group.landmark" as "is__landmark_indigenous_and_community_lands",
      $"data_group.wdpa" as "wdpa_protected_areas__iucn_cat",
      $"data_group.intactForestLandscapes" as "ifl_intact_forest_landscapes__year",
      $"data_group.plantationsTypeFluxModel" as "gfw_plantation_flux_model__type",
      $"data_group.intactPrimaryForest" as "is__intact_primary_forest",
      $"data_group.peatlandsExtentFluxModel" as "is__peatlands_extent_flux_model",
      $"data_group.forestAgeCategory" as "gfw_forest_age_category__cat",
      $"data_group.jplTropicsAbovegroundBiomassExtent2000" as "is__jpl_aboveground_biomass_extent",
      $"data_group.fiaRegionsUsExtent" as "usfs_fia_region__name",
      $"data_group.braBiomes" as "bra_biome__name",
      $"data_group.riverBasins" as "mapbox_river_basin__name",
      $"data_group.primaryForest" as "is__umd_regional_primary_forest_2001",
      $"data_group.isLossLegalAmazon" as "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
      $"data_group.prodesLegalAmazonExtent2000" as "is__prodes_legal_Amazon_extent_2000",
      $"data_group.tropicLatitudeExtent" as "is__tropical_latitude_extent",
      $"data_group.isBurnLoss" as "is__gfw_burn_loss_2001-2021",
      $"data_group.grossEmissionsNodeCodes" as "gfw_gross_emissions_code__type",

      $"data.totalTreecoverLoss" as "umd_tree_cover_loss__ha",
      $"data.totalBiomassLoss" as "whrc_aboveground_biomass_loss__Mg_AGB",
      $"data.totalGrossEmissionsCo2eCo2OnlyBiomassSoil" as "gfw_forest_carbon_gross_emissions_co2_only_biomass_soil__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eNonCo2BiomassSoil" as "gfw_forest_carbon_gross_emissions_non_co2_biomass_soil__Mg_CO2e",
      $"data.totalGrossEmissionsCo2eBiomassSoil" as "gfw_forest_carbon_gross_emissions_biomass_soil__Mg_CO2e",
      $"data.totalAgcEmisYear" as "gfw_aboveground_carbon_stock_in_emissions_year__Mg",
      $"data.totalBgcEmisYear" as "gfw_belowground_carbon_stock_in_emissions_year__Mg",
      $"data.totalDeadwoodCarbonEmisYear" as "gfw_deadwood_carbon_stock_in_emissions_year__Mg",
      $"data.totalLitterCarbonEmisYear" as "gfw_litter_carbon_stock_in_emissions_year__Mg",
      $"data.totalSoilCarbonEmisYear" as "gfw_soil_carbon_stock_in_emissions_year__Mg",
      $"data.totalCarbonEmisYear" as "gfw_total_carbon_stock_in_emissions_year__Mg",
      $"data.totalTreecoverExtent2000" as "umd_tree_cover_extent_2000__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalBiomass" as "whrc_aboveground_biomass_stock_2000__Mg",
      $"data.totalGrossAnnualAbovegroundRemovalsCarbon" as "gfw_forest_carbon_annual_removals_aboveground__Mg_C",
      $"data.totalGrossAnnualBelowgroundRemovalsCarbon" as "gfw_forest_carbon_annual_removals_belowground__Mg_C",
      $"data.totalGrossAnnualAboveBelowgroundRemovalsCarbon" as "gfw_forest_carbon_annual_removals__Mg_C",
      $"data.totalGrossCumulAbovegroundRemovalsCo2" as "gfw_forest_carbon_gross_removals_aboveground__Mg_CO2",
      $"data.totalGrossCumulBelowgroundRemovalsCo2" as "gfw_forest_carbon_gross_removals_belowground__Mg_CO2",
      $"data.totalGrossCumulAboveBelowgroundRemovalsCo2" as "gfw_forest_carbon_gross_removals__Mg_CO2",
      $"data.totalNetFluxCo2" as "gfw_forest_carbon_net_flux__Mg_CO2e",
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
        sum("gfw_forest_carbon_annual_removals_aboveground__Mg_C") as "gfw_forest_carbon_annual_removals_aboveground__Mg_C",
        sum("gfw_forest_carbon_annual_removals_belowground__Mg_C") as "gfw_forest_carbon_annual_removals_belowground__Mg_C",
        sum("gfw_forest_carbon_annual_removals__Mg_C") as "gfw_forest_carbon_annual_removals__Mg_C",
        sum("gfw_forest_carbon_gross_removals_aboveground__Mg_CO2") as "gfw_forest_carbon_gross_removals_aboveground__Mg_CO2",
        sum("gfw_forest_carbon_gross_removals_belowground__Mg_CO2") as "gfw_forest_carbon_gross_removals_belowground__Mg_CO2",
        sum("gfw_forest_carbon_gross_removals__Mg_CO2") as "gfw_forest_carbon_gross_removals__Mg_CO2",
        sum("gfw_forest_carbon_net_flux__Mg_CO2e") as "gfw_forest_carbon_net_flux__Mg_CO2e",
        sum("gfw_aboveground_carbon_stock_2000__Mg") as "gfw_aboveground_carbon_stock_2000__Mg",
        sum("gfw_belowground_carbon_stock_2000__Mg") as "gfw_belowground_carbon_stock_2000__Mg",
        sum("gfw_deadwood_carbon_stock_2000__Mg") as "gfw_deadwood_carbon_stock_2000__Mg",
        sum("gfw_litter_carbon_stock_2000__Mg") as "gfw_litter_carbon_stock_2000__Mg",
        sum("gfw_soil_carbon_stock_2000__Mg") as "gfw_soil_carbon_stock_2000__Mg",
        sum("gfw_total_carbon_stock_2000__Mg") as "gfw_total_carbon_stock_2000__Mg",
        sum("umd_tree_cover_loss__ha") as "umd_tree_cover_loss__ha",
        sum("whrc_aboveground_biomass_loss__Mg_AGB") as "whrc_aboveground_biomass_loss__Mg_AGB",
        sum("gfw_forest_carbon_gross_emissions_co2_only_biomass_soil__Mg_CO2e") as "gfw_forest_carbon_gross_emissions_co2_only_biomass_soil__Mg_CO2e",
        sum("gfw_forest_carbon_gross_emissions_non_co2_biomass_soil__Mg_CO2e") as "gfw_forest_carbon_gross_emissions_non_co2_biomass_soil__Mg_CO2e",
        sum("gfw_forest_carbon_gross_emissions_biomass_soil__Mg_CO2e") as "gfw_forest_carbon_gross_emissions_biomass_soil__Mg_CO2e",
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
        sum("gfw_forest_carbon_annual_removals_aboveground__Mg_C") as "gfw_forest_carbon_annual_removals_aboveground__Mg_C",
        sum("gfw_forest_carbon_annual_removals_belowground__Mg_C") as "gfw_forest_carbon_annual_removals_belowground__Mg_C",
        sum("gfw_forest_carbon_annual_removals__Mg_C") as "gfw_forest_carbon_annual_removals__Mg_C",
        sum("gfw_forest_carbon_gross_removals_aboveground__Mg_CO2") as "gfw_forest_carbon_gross_removals_aboveground__Mg_CO2",
        sum("gfw_forest_carbon_gross_removals_belowground__Mg_CO2") as "gfw_forest_carbon_gross_removals_belowground__Mg_CO2",
        sum("gfw_forest_carbon_gross_removals__Mg_CO2") as "gfw_forest_carbon_gross_removals__Mg_CO2",
        sum("gfw_forest_carbon_net_flux__Mg_CO2e") as "gfw_forest_carbon_net_flux__Mg_CO2e",
        sum("gfw_aboveground_carbon_stock_2000__Mg") as "gfw_aboveground_carbon_stock_2000__Mg",
        sum("gfw_belowground_carbon_stock_2000__Mg") as "gfw_belowground_carbon_stock_2000__Mg",
        sum("gfw_deadwood_carbon_stock_2000__Mg") as "gfw_deadwood_carbon_stock_2000__Mg",
        sum("gfw_litter_carbon_stock_2000__Mg") as "gfw_litter_carbon_stock_2000__Mg",
        sum("gfw_soil_carbon_stock_2000__Mg") as "gfw_soil_carbon_stock_2000__Mg",
        sum("gfw_total_carbon_stock_2000__Mg") as "gfw_total_carbon_stock_2000__Mg",
        sum("umd_tree_cover_loss__ha") as "umd_tree_cover_loss__ha",
        sum("whrc_aboveground_biomass_loss__Mg_AGB") as "whrc_aboveground_biomass_loss__Mg_AGB",
        sum("gfw_forest_carbon_gross_emissions_co2_only_biomass_soil__Mg_CO2e") as "gfw_forest_carbon_gross_emissions_co2_only_biomass_soil__Mg_CO2e",
        sum("gfw_forest_carbon_gross_emissions_non_co2_biomass_soil__Mg_CO2e") as "gfw_forest_carbon_gross_emissions_non_co2_biomass_soil__Mg_CO2e",
        sum("gfw_forest_carbon_gross_emissions_biomass_soil__Mg_CO2e") as "gfw_forest_carbon_gross_emissions_biomass_soil__Mg_CO2e",
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
        sum("gfw_forest_carbon_gross_emissions_co2_only_biomass_soil__Mg_CO2e") as "gfw_forest_carbon_gross_emissions_co2_only_biomass_soil__Mg_CO2e",
        sum("gfw_forest_carbon_gross_emissions_non_co2_biomass_soil__Mg_CO2e") as "gfw_forest_carbon_gross_emissions_non_co2_biomass_soil__Mg_CO2e",
        sum("gfw_forest_carbon_gross_emissions_biomass_soil__Mg_CO2e") as "gfw_forest_carbon_gross_emissions_biomass_soil__Mg_CO2e",
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
        max($"is__flux_model_extent") as "is__flux_model_extent",
        max(length($"flux_removal_forest_type__name")).cast("boolean") as "flux_removal_forest_type__name",
        max($"is__umd_tree_cover_loss") as "is__umd_tree_cover_loss",
        max($"is__umd_tree_cover_gain") as "is__umd_tree_cover_gain",
        max($"is__gmw_mangroves_extent") as "is__gmw_mangroves_extent",
        max(length($"tsc_tree_cover_loss_drivers__driver")).cast("boolean") as "tsc_tree_cover_loss_drivers__driver",
        max(length($"fao_ecozones__class")).cast("boolean") as "fao_ecozones__class",
        max($"is__landmark_indigenous_and_community_lands") as "is__landmark_indigenous_and_community_lands",
        max(length($"wdpa_protected_areas__iucn_cat")).cast("boolean") as "wdpa_protected_areas__iucn_cat",
        max(length($"ifl_intact_forest_landscapes__year")).cast("boolean") as "ifl_intact_forest_landscapes__year",
        max(length($"gfw_plantation_flux_model__type")).cast("boolean") as "gfw_plantation_flux_model__type",
        max($"is__intact_primary_forest") as "is__intact_primary_forest",
        max($"is__peatlands_extent_flux_model") as "is__peatlands_extent_flux_model",
        max(length($"gfw_forest_age_category__cat")).cast("boolean") as "gfw_forest_age_category__cat",
        max($"is__jpl_aboveground_biomass_extent") as "is__jpl_aboveground_biomass_extent",
        max(length($"usfs_fia_region__name")).cast("boolean") as "usfs_fia_region__name",
        max(length($"bra_biome__name")).cast("boolean") as "bra_biome__name",
        max(length($"mapbox_river_basin__name")).cast("boolean") as "mapbox_river_basin__name",
        max($"is__umd_regional_primary_forest_2001") as "is__umd_regional_primary_forest_2001",
        max($"is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019") as "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
        max($"is__prodes_legal_Amazon_extent_2000") as "is__prodes_legal_Amazon_extent_2000",
        max($"is__tropical_latitude_extent") as "is__tropical_latitude_extent",
        max($"is__gfw_burn_loss_2001-2021") as "is__gfw_burn_loss_2001-2021",
        max(length($"gfw_gross_emissions_code__type")).cast("boolean") as "gfw_gross_emissions_code__type"
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
        max($"is__gmw_mangroves_extent") as "is__gmw_mangroves_extent",
        max($"tsc_tree_cover_loss_drivers__driver") as "tsc_tree_cover_loss_drivers__driver",
        max($"fao_ecozones__class") as "fao_ecozones__class",
        max($"is__landmark_indigenous_and_community_lands") as "is__landmark_indigenous_and_community_lands",
        max($"wdpa_protected_areas__iucn_cat") as "wdpa_protected_areas__iucn_cat",
        max($"ifl_intact_forest_landscapes__year") as "ifl_intact_forest_landscapes__year",
        max($"gfw_plantation_flux_model__type") as "gfw_plantation_flux_model__type",
        max($"is__intact_primary_forest") as "is__intact_primary_forest",
        max($"is__peatlands_extent_flux_model") as "is__peatlands_extent_flux_model",
        max($"gfw_forest_age_category__cat") as "gfw_forest_age_category__cat",
        max($"is__jpl_aboveground_biomass_extent") as "is__jpl_aboveground_biomass_extent",
        max($"usfs_fia_region__name") as "usfs_fia_region__name",
        max($"bra_biome__name") as "bra_biome__name",
        max($"mapbox_river_basin__name") as "mapbox_river_basin__name",
        max($"is__umd_regional_primary_forest_2001") as "is__umd_regional_primary_forest_2001",
        max($"is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019") as "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
        max($"is__prodes_legal_Amazon_extent_2000") as "is__prodes_legal_Amazon_extent_2000",
        max($"is__tropical_latitude_extent") as "is__tropical_latitude_extent",
        max($"is__gfw_burn_loss_2001-2021") as "is__gfw_burn_loss_2001-2021",
        max($"gfw_gross_emissions_code__type") as "gfw_gross_emissions_code__type"
      )
  }


}
