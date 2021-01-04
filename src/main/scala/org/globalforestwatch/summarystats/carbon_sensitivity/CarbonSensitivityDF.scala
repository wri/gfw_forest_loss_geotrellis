package org.globalforestwatch.summarystats.carbon_sensitivity

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.{length, max, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CarbonSensitivityDF {

    val contextualLayers: List[String] = List(
      "is__flux_model_extent",
      "flux_removal_forest_type__name",
      "umd_tree_cover_density__threshold",
      "is__umd_tree_cover_loss_2000-2019",
      "is__umd_tree_cover_gain_2000-2012",
      "is__gmw_mangroves_extent",
      "tsc_tree_cover_loss_drivers__type",
      "wwf_eco_region__name",
      "is__gfw_land_right",
      "wdpa_protected_area__iucn_cat",
      "ifl_intact_forest_landscape__year",
      "gfw_plantation_flux_model__type",
      "is__intact_primary_forest",
      "is__peatlands_extent_flux_model",
      "gfw_forest_age_category__cat",
      "is__jpl_aboveground_biomass_extent",
      "usfs_fia_region__name",
      "bra_biome__name",
      "mapbox_river_basin__name",
      "is__regional_primary_forest",
      "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
      "is__prodes_legal_Amazon_extent_2000",
      "is__tropical_latitude_extent",
      "is__gfw_burn_loss_2001-19",
      "gfw_gross_emissions_code__type"
    )

    def unpackValues(df: DataFrame): DataFrame = {

      implicit val spark: SparkSession = df.sparkSession
      import spark.implicits._

      validatePresenceOfColumns(df, Seq("id", "dataGroup", "data"))

      df.select(
        $"id.iso" as "iso",
        $"id.adm1" as "adm1",
        $"id.adm2" as "adm2",
        $"dataGroup.fluxModelExtent" as "is__flux_model_extent",
        $"dataGroup.removalForestType" as "flux_removal_forest_type__name",
        $"dataGroup.lossYear" as "umd_tree_cover_loss__year",
        $"dataGroup.threshold" as "umd_tree_cover_density__threshold",
        $"dataGroup.isGain" as "is__umd_tree_cover_gain_2000-2012",
        $"dataGroup.isLoss" as "is__umd_tree_cover_loss_2000-2019",
        $"dataGroup.mangroveBiomassExtent" as "is__gmw_mangroves_extent",
        $"dataGroup.drivers" as "tsc_tree_cover_loss_drivers__type",
        $"dataGroup.ecozones" as "wwf_eco_region__name",
        $"dataGroup.landRights" as "is__gfw_land_right",
        $"dataGroup.wdpa" as "wdpa_protected_area__iucn_cat",
        $"dataGroup.intactForestLandscapes" as "ifl_intact_forest_landscape__year",
        $"dataGroup.plantationsTypeFluxModel" as "gfw_plantation_flux_model__type",
        $"dataGroup.intactPrimaryForest" as "is__intact_primary_forest",
        $"dataGroup.peatlandsExtentFluxModel" as "is__peatlands_extent_flux_model",
        $"dataGroup.forestAgeCategory" as "gfw_forest_age_category__cat",
        $"dataGroup.jplTropicsAbovegroundBiomassExtent2000" as "is__jpl_aboveground_biomass_extent",
        $"dataGroup.fiaRegionsUsExtent" as "usfs_fia_region__name",
        $"dataGroup.braBiomes" as "bra_biome__name",
        $"dataGroup.riverBasins" as "mapbox_river_basin__name",
        $"dataGroup.primaryForest" as "is__regional_primary_forest",
        $"dataGroup.isLossLegalAmazon" as "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
        $"dataGroup.prodesLegalAmazonExtent2000" as "is__prodes_legal_Amazon_extent_2000",
        $"dataGroup.tropicLatitudeExtent" as "is__tropical_latitude_extent",
        $"dataGroup.isBurnLoss" as "is__gfw_burn_loss_2001-19",
        $"dataGroup.grossEmissionsNodeCodes" as "gfw_gross_emissions_code__type",

        $"data.totalTreecoverLoss" as "umd_tree_cover_loss__ha",
        $"data.totalBiomassLoss" as "whrc_aboveground_biomass_loss__Mg",
        $"data.totalGrossEmissionsCo2eCo2Only" as "gfw_gross_emissions_co2e_co2_only__Mg",
        $"data.totalGrossEmissionsCo2eNonCo2" as "gfw_gross_emissions_co2e_non_co2__Mg",
        $"data.totalGrossEmissionsCo2e" as "gfw_gross_emissions_co2e_all_gases__Mg",
        $"data.totalAgcEmisYear" as "gfw_aboveground_carbon_stock_in_emissions_year__Mg",
        $"data.totalSoilCarbonEmisYear" as "gfw_soil_carbon_stock_in_emissions_year__Mg",
        $"data.totalTreecoverExtent2000" as "umd_tree_cover_extent_2000__ha",
        $"data.totalArea" as "area__ha",
        $"data.totalBiomass" as "whrc_aboveground_biomass_stock_2000__Mg",
        $"data.totalGrossCumulAbovegroundRemovalsCo2" as "gfw_gross_cumulative_aboveground_co2_removals__Mg",
        $"data.totalGrossCumulBelowgroundRemovalsCo2" as "gfw_gross_cumulative_belowground_co2_removals__Mg",
        $"data.totalGrossCumulAboveBelowgroundRemovalsCo2" as "gfw_gross_cumulative_aboveground_belowground_co2_removals__Mg",
        $"data.totalNetFluxCo2" as "gfw_net_flux_co2e__Mg",
        $"data.totalJplTropicsAbovegroundBiomassDensity2000" as "jpl_tropics_aboveground_biomass_2000__Mg",
        $"data.totalTreecoverLossLegalAmazon" as "legal_amazon_umd_tree_cover_loss__ha",
        $"data.totalFluxModelExtentArea" as "gfw_flux_model_extent__ha"
      )
    }

    def aggSummary(groupByCols: List[String])(df: DataFrame): DataFrame = {

      df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
        .agg(
          sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
          sum("area__ha") as "area__ha",
          sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
          sum("gfw_gross_cumulative_aboveground_co2_removals__Mg") as "gfw_gross_cumulative_aboveground_co2_removals_2001-2019__Mg",
          sum("gfw_gross_cumulative_belowground_co2_removals__Mg") as "gfw_gross_cumulative_belowground_co2_removals_2001-2019__Mg",
          sum("gfw_gross_cumulative_aboveground_belowground_co2_removals__Mg") as "gfw_gross_cumulative_aboveground_belowground_co2_removals_2001-2019__Mg",
          sum("gfw_net_flux_co2e__Mg") as "gfw_net_flux_co2e_2001-2019__Mg",
          sum("umd_tree_cover_loss__ha") as "treecover_loss_2001-2019__ha",
          sum("whrc_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2001-2019__Mg",
          sum("gfw_gross_emissions_co2e_co2_only__Mg") as "gfw_gross_emissions_co2e_co2_only_2001-2019__Mg",
          sum("gfw_gross_emissions_co2e_non_co2__Mg") as "gfw_gross_emissions_co2e_non_co2_2001-2019__Mg",
          sum("gfw_gross_emissions_co2e_all_gases__Mg") as "gfw_gross_emissions_co2e_all_gases_2001-2019__Mg",
          sum("jpl_tropics_aboveground_biomass_2000__Mg") as "jpl_tropics_aboveground_biomass_2000__Mg",
          sum("legal_amazon_umd_tree_cover_loss__ha") as "legal_amazon_umd_tree_cover_loss_2001-2019__ha",
          sum("gfw_flux_model_extent__ha") as "gfw_flux_model_extent__ha"

        )
    }

    def aggSummary2(groupByCols: List[String])(df: DataFrame): DataFrame = {

      df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
        .agg(
          sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
          sum("area__ha") as "area__ha",
          sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
          sum("gfw_gross_cumulative_aboveground_co2_removals_2001-2019__Mg") as "gfw_gross_cumulative_aboveground_co2_removals_2001-2019__Mg",
          sum("gfw_gross_cumulative_belowground_co2_removals_2001-2019__Mg") as "gfw_gross_cumulative_belowground_co2_removals_2001-2019__Mg",
          sum("gfw_gross_cumulative_aboveground_belowground_co2_removals_2001-2019__Mg") as "gfw_gross_cumulative_aboveground_belowground_co2_removals_2001-2019__Mg",
          sum("gfw_net_flux_co2e_2001-2019__Mg") as "gfw_net_flux_co2e_2001-2019__Mg",
          sum("treecover_loss_2001-2019__ha") as "treecover_loss_2001-2019__ha",
          sum("aboveground_biomass_loss_2001-2019__Mg") as "aboveground_biomass_loss_2001-2019__Mg",
          sum("gfw_gross_emissions_co2e_co2_only_2001-2019__Mg") as "gfw_gross_emissions_co2e_co2_only_2001-2019__Mg",
          sum("gfw_gross_emissions_co2e_non_co2_2001-2019__Mg") as "gfw_gross_emissions_co2e_non_co2_2001-2019__Mg",
          sum("gfw_gross_emissions_co2e_all_gases_2001-2019__Mg") as "gfw_gross_emissions_co2e_all_gases_2001-2019__Mg",
          sum("jpl_tropics_aboveground_biomass_2000__Mg") as "jpl_tropics_aboveground_biomass_2000__Mg",
          sum("legal_amazon_umd_tree_cover_loss_2001-2019__ha") as "legal_amazon_umd_tree_cover_loss_2001-2019__ha",
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
          sum("whrc_aboveground_biomass_loss__Mg") as "whrc_aboveground_biomass_loss__Mg",
          sum("gfw_gross_emissions_co2e_co2_only__Mg") as "gfw_gross_emissions_co2e_co2_only__Mg",
          sum("gfw_gross_emissions_co2e_non_co2__Mg") as "gfw_gross_emissions_co2e_non_co2__Mg",
          sum("gfw_gross_emissions_co2e_all_gases__Mg") as "gfw_gross_emissions_co2e_all_gases__Mg",
          sum("gfw_aboveground_carbon_stock_in_emissions_year__Mg") as "gfw_aboveground_carbon_stock_in_emissions_year__Mg",
          sum("gfw_soil_carbon_stock_in_emissions_year__Mg") as "gfw_soil_carbon_stock_in_emissions_year__Mg"
        )
    }

    def whitelist(groupByCols: List[String])(df: DataFrame): DataFrame = {

      val spark = df.sparkSession
      import spark.implicits._

      df.groupBy(groupByCols.head, groupByCols.tail: _*)
        .agg(
          max($"is__flux_model_extent") as "is__flux_model_extent",
          max(length($"flux_removal_forest_type__name")).cast("boolean") as "flux_removal_forest_type__name",
          max($"is__umd_tree_cover_loss_2000-2019") as "is__umd_tree_cover_loss_2000-2019",
          max($"is__umd_tree_cover_gain_2000-2012") as "is__umd_tree_cover_gain_2000-2012",
          max($"is__gmw_mangroves_extent") as "is__gmw_mangroves_extent",
          max(length($"tsc_tree_cover_loss_drivers__type")).cast("boolean") as "tsc_tree_cover_loss_drivers__type",
          max(length($"wwf_eco_region__name")).cast("boolean") as "wwf_eco_region__name",
          max($"is__gfw_land_right") as "is__gfw_land_right",
          max(length($"wdpa_protected_area__iucn_cat")).cast("boolean") as "wdpa_protected_area__iucn_cat",
          max(length($"ifl_intact_forest_landscape__year")).cast("boolean") as "ifl_intact_forest_landscape__year",
          max(length($"gfw_plantation_flux_model__type")).cast("boolean") as "gfw_plantation_flux_model__type",
          max($"is__intact_primary_forest") as "is__intact_primary_forest",
          max($"is__peatlands_extent_flux_model") as "is__peatlands_extent_flux_model",
          max(length($"gfw_forest_age_category__cat")).cast("boolean") as "gfw_forest_age_category__cat",
          max($"is__jpl_aboveground_biomass_extent") as "is__jpl_aboveground_biomass_extent",
          max(length($"usfs_fia_region__name")).cast("boolean") as "usfs_fia_region__name",
          max(length($"bra_biome__name")).cast("boolean") as "bra_biome__name",
          max(length($"mapbox_river_basin__name")).cast("boolean") as "mapbox_river_basin__name",
          max($"is__regional_primary_forest") as "is__regional_primary_forest",
          max($"is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019") as "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
          max($"is__prodes_legal_Amazon_extent_2000") as "is__prodes_legal_Amazon_extent_2000",
          max($"is__tropical_latitude_extent") as "is__tropical_latitude_extent",
          max($"is__gfw_burn_loss_2001-19") as "is__gfw_burn_loss_2001-19",
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
          max($"is__umd_tree_cover_loss_2000-2019") as "is__umd_tree_cover_loss_2000-2019",
          max($"is__umd_tree_cover_gain_2000-2012") as "is__umd_tree_cover_gain_2000-2012",
          max($"is__gmw_mangroves_extent") as "is__gmw_mangroves_extent",
          max($"tsc_tree_cover_loss_drivers__type") as "tsc_tree_cover_loss_drivers__type",
          max($"wwf_eco_region__name") as "wwf_eco_region__name",
          max($"is__gfw_land_right") as "is__gfw_land_right",
          max($"wdpa_protected_area__iucn_cat") as "wdpa_protected_area__iucn_cat",
          max($"ifl_intact_forest_landscape__year") as "ifl_intact_forest_landscape__year",
          max($"gfw_plantation_flux_model__type") as "gfw_plantation_flux_model__type",
          max($"is__intact_primary_forest") as "is__intact_primary_forest",
          max($"is__peatlands_extent_flux_model") as "is__peatlands_extent_flux_model",
          max($"gfw_forest_age_category__cat") as "gfw_forest_age_category__cat",
          max($"is__jpl_aboveground_biomass_extent") as "is__jpl_aboveground_biomass_extent",
          max($"usfs_fia_region__name") as "usfs_fia_region__name",
          max($"bra_biome__name") as "bra_biome__name",
          max($"mapbox_river_basin__name") as "mapbox_river_basin__name",
          max($"is__regional_primary_forest") as "is__regional_primary_forest",
          max($"is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019") as "is__prodes_legal_amazon_umd_tree_cover_loss_2001-2019",
          max($"is__prodes_legal_Amazon_extent_2000") as "is__prodes_legal_Amazon_extent_2000",
          max($"is__tropical_latitude_extent") as "is__tropical_latitude_extent",
          max($"is__gfw_burn_loss_2001-19") as "is__gfw_burn_loss_2001-19",
          max($"gfw_gross_emissions_code__type") as "gfw_gross_emissions_code__type"
        )
    }
}
