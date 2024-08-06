package org.globalforestwatch.summarystats.integrated_alerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object IntegratedAlertsDF {

  val contextualLayers: List[String] = List(
    //"umd_glad_landsat_alerts__date",
    "umd_glad_sentinel2_alerts__date",
    "wur_radd_alerts__date",
    "gfw_integrated_alerts__date",
    //"umd_glad_landsat_alerts__confidence",
    "umd_glad_sentinel2_alerts__confidence",
    "wur_radd_alerts__confidence",
    "gfw_integrated_alerts__confidence",
    "is__umd_regional_primary_forest_2001",
    "is__birdlife_alliance_for_zero_extinction_sites",
    "is__birdlife_key_biodiversity_areas",
    "is__landmark_indigenous_and_community_lands",
    "gfw_planted_forests__type",
    "is__gfw_mining_concessions",
    "is__gfw_managed_forests",
    "rspo_oil_palm__certification_status",
    "is__gfw_wood_fiber",
    "is__gfw_peatlands",
    "is__idn_forest_moratorium",
    "is__gfw_oil_palm",
    "idn_forest_area__class",
    "per_forest_concessions__type",
    "is__gfw_oil_gas",
    "is__gmw_global_mangrove_extent_2020",
    "is__ifl_intact_forest_landscapes_2016",
    "ibge_bra_biomes__name",

    "is__birdlife_alliance_for_zero_extinction_site",
    "is__birdlife_key_biodiversity_area",
    "is__landmark_land_right",
    "gfw_plantation__type",
    "is__gfw_mining",
    "is__gfw_managed_forest",
    "is__peatland",
    "idn_forest_area__type",
    "per_forest_concession__type",
    "is__gmw_mangroves_2020",
    "is__ifl_intact_forest_landscape_2016",
    "bra_biome__name",
    "sbtn_natural_forests__class"
  )

  def unpackValues(unpackCols: List[Column],
                   wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    def defaultCols =
      List(
        //$"data_group.gladLAlertDate" as "umd_glad_landsat_alerts__date",
        $"data_group.gladS2AlertDate" as "umd_glad_sentinel2_alerts__date",
        $"data_group.raddAlertDate" as "wur_radd_alerts__date",
        $"data_group.integratedAlertDate" as "gfw_integrated_alerts__date",
        //$"data_group.gladLConfidence" as "umd_glad_landsat_alerts__confidence",
        $"data_group.gladS2Confidence" as "umd_glad_sentinel2_alerts__confidence",
        $"data_group.raddConfidence" as "wur_radd_alerts__confidence",
        $"data_group.integratedConfidence" as "gfw_integrated_alerts__confidence",
        $"data_group.primaryForest" as "is__umd_regional_primary_forest_2001",
        $"data_group.aze" as "is__birdlife_alliance_for_zero_extinction_sites",
        $"data_group.keyBiodiversityAreas" as "is__birdlife_key_biodiversity_areas",
        $"data_group.landmark" as "is__landmark_indigenous_and_community_lands",
        $"data_group.plantedForests" as "gfw_planted_forests__type",
        $"data_group.mining" as "is__gfw_mining_concessions",
        $"data_group.logging" as "is__gfw_managed_forests",
        $"data_group.rspo" as "rspo_oil_palm__certification_status",
        $"data_group.woodFiber" as "is__gfw_wood_fiber",
        $"data_group.peatlands" as "is__gfw_peatlands",
        $"data_group.indonesiaForestMoratorium" as "is__idn_forest_moratorium",
        $"data_group.oilPalm" as "is__gfw_oil_palm",
        $"data_group.indonesiaForestArea" as "idn_forest_area__class",
        $"data_group.peruForestConcessions" as "per_forest_concessions__type",
        $"data_group.oilGas" as "is__gfw_oil_gas",
        $"data_group.mangroves2020" as "is__gmw_global_mangrove_extent_2020",
        $"data_group.intactForestLandscapes2016" as "is__ifl_intact_forest_landscapes_2016",
        $"data_group.braBiomes" as "ibge_bra_biomes__name",
        $"data.totalAlerts" as "alert__count",
        $"data.alertArea" as "alert_area__ha",
        $"data.co2Emissions" as "whrc_aboveground_co2_emissions__Mg",
        $"data.totalArea" as "area__ha",

        $"data_group.aze" as "is__birdlife_alliance_for_zero_extinction_site",
        $"data_group.keyBiodiversityAreas" as "is__birdlife_key_biodiversity_area",
        $"data_group.landmark" as "is__landmark_land_right",
        $"data_group.plantedForests" as "gfw_plantation__type",
        $"data_group.mining" as "is__gfw_mining",
        $"data_group.logging" as "is__gfw_managed_forest",
        $"data_group.peatlands" as "is__peatland",
        $"data_group.indonesiaForestArea" as "idn_forest_area__type",
        $"data_group.peruForestConcessions" as "per_forest_concession__type",
        $"data_group.mangroves2020" as "is__gmw_mangroves_2020",
        $"data_group.intactForestLandscapes2016" as "is__ifl_intact_forest_landscape_2016",
        $"data_group.braBiomes" as "bra_biome__name",
        $"data_group.naturalForests" as "sbtn_natural_forests__class",
      )

    val cols =
      if (!wdpa)
        unpackCols ::: ($"data_group.protectedAreas" as "wdpa_protected_areas__iucn_cat") :: ($"data_group.protectedAreas" as "wdpa_protected_area__iucn_cat") :: defaultCols
      else unpackCols ::: defaultCols

    df.select(cols: _*)
  }

  def aggChangeDaily(groupByCols: List[String],
                     wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val cols =
      if (!wdpa)
        groupByCols ::: "wdpa_protected_areas__iucn_cat" :: "wdpa_protected_area__iucn_cat" :: contextualLayers
      else
        groupByCols ::: contextualLayers

    df//.filter($"gfw_integrated_alerts__confidence".notEqual("not_detected"))
      .groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("alert__count") as "alert__count",
        sum("alert_area__ha") as "alert_area__ha",
        sum("whrc_aboveground_co2_emissions__Mg") as "whrc_aboveground_co2_emissions__Mg"
      )
  }

  def aggSummary(groupByCols: List[String],
                 wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val cols =
      if (!wdpa)
        groupByCols ::: "wdpa_protected_areas__iucn_cat" :: "wdpa_protected_area__iucn_cat" :: contextualLayers
      else
        groupByCols ::: contextualLayers

    df.groupBy(cols.head, cols.tail: _*)
      .agg(sum("area__ha") as "area__ha")
  }

  def whitelist(groupByCols: List[String],
                wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val defaultAggCols = List(
      max("is__umd_regional_primary_forest_2001") as "is__umd_regional_primary_forest_2001",
      max("is__birdlife_alliance_for_zero_extinction_sites") as "is__birdlife_alliance_for_zero_extinction_sites",
      max("is__birdlife_key_biodiversity_areas") as "is__birdlife_key_biodiversity_areas",
      max("is__landmark_indigenous_and_community_lands") as "is__landmark_indigenous_and_community_lands",
      max(length($"gfw_planted_forests__type"))
        .cast("boolean") as "gfw_planted_forests__type",
      max("is__gfw_mining_concessions") as "is__gfw_mining_concessions",
      max("is__gfw_managed_forests") as "is__gfw_managed_forests",
      max(length($"rspo_oil_palm__certification_status"))
        .cast("boolean") as "rspo_oil_palm__certification_status",
      max("is__gfw_wood_fiber") as "is__gfw_wood_fiber",
      max("is__gfw_peatlands") as "is__gfw_peatlands",
      max("is__idn_forest_moratorium") as "is__idn_forest_moratorium",
      max("is__gfw_oil_palm") as "is__gfw_oil_palm",
      max(length($"idn_forest_area__class"))
        .cast("boolean") as "idn_forest_area__class",
      max(length($"per_forest_concessions__type"))
        .cast("boolean") as "per_forest_concessions__type",
      max("is__gfw_oil_gas") as "is__gfw_oil_gas",
      max("is__gmw_global_mangrove_extent_2020") as "is__gmw_global_mangrove_extent_2020",
      max("is__ifl_intact_forest_landscapes_2016") as "is__ifl_intact_forest_landscapes_2016",
      max(length($"ibge_bra_biomes__name")).cast("boolean") as "ibge_bra_biomes__name",

      max("is__birdlife_alliance_for_zero_extinction_site") as "is__birdlife_alliance_for_zero_extinction_site",
      max("is__birdlife_key_biodiversity_area") as "is__birdlife_key_biodiversity_area",
      max("is__landmark_land_right") as "is__landmark_land_right",
      max(length($"gfw_plantation__type"))
        .cast("boolean") as "gfw_plantation__type",
      max("is__gfw_mining") as "is__gfw_mining",
      max("is__gfw_managed_forest") as "is__gfw_managed_forest",
      max("is__peatland") as "is__peatland",
      max(length($"idn_forest_area__type"))
        .cast("boolean") as "idn_forest_area__type",
      max(length($"per_forest_concession__type"))
        .cast("boolean") as "per_forest_concession__type",
      max("is__gmw_mangroves_2020") as "is__gmw_mangroves_2020",
      max("is__ifl_intact_forest_landscape_2016") as "is__ifl_intact_forest_landscape_2016",
      max(length($"bra_biome__name")).cast("boolean") as "bra_biome__name",
      max(length($"sbtn_natural_forests__class")).cast("boolean") as "sbtn_natural_forests__class"
    )

    val aggCols =
      if (!wdpa)
        (max(length($"wdpa_protected_areas__iucn_cat"))
          .cast("boolean") as "wdpa_protected_areas__iucn_cat") ::
          (max(length($"wdpa_protected_area__iucn_cat"))
            .cast("boolean") as "wdpa_protected_area__iucn_cat") :: defaultAggCols
      else defaultAggCols

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(aggCols.head, aggCols.tail: _*)
  }

  def whitelist2(groupByCols: List[String],
                 wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val defaultAggCols = List(
      max("is__umd_regional_primary_forest_2001") as "is__umd_regional_primary_forest_2001",
      max("is__birdlife_alliance_for_zero_extinction_sites") as "is__birdlife_alliance_for_zero_extinction_sites",
      max("is__birdlife_key_biodiversity_areas") as "is__birdlife_key_biodiversity_areas",
      max("is__landmark_indigenous_and_community_lands") as "is__landmark_indigenous_and_community_lands",
      max("gfw_planted_forests__type") as "gfw_planted_forests__type",
      max("is__gfw_mining_concessions") as "is__gfw_mining_concessions",
      max("is__gfw_managed_forests") as "is__gfw_managed_forests",
      max("rspo_oil_palm__certification_status") as "rspo_oil_palm__certification_status",
      max("is__gfw_wood_fiber") as "is__gfw_wood_fiber",
      max("is__gfw_peatlands") as "is__gfw_peatlands",
      max("is__idn_forest_moratorium") as "is__idn_forest_moratorium",
      max("is__gfw_oil_palm") as "is__gfw_oil_palm",
      max("idn_forest_area__class") as "idn_forest_area__class",
      max("per_forest_concessions__type") as "per_forest_concessions__type",
      max("is__gfw_oil_gas") as "is__gfw_oil_gas",
      max("is__gmw_global_mangrove_extent_2020") as "is__gmw_global_mangrove_extent_2020",
      max("is__ifl_intact_forest_landscapes_2016") as "is__ifl_intact_forest_landscapes_2016",
      max("ibge_bra_biomes__name") as "ibge_bra_biomes__name",

      max("is__birdlife_alliance_for_zero_extinction_site") as "is__birdlife_alliance_for_zero_extinction_site",
      max("is__birdlife_key_biodiversity_area") as "is__birdlife_key_biodiversity_area",
      max("is__landmark_land_right") as "is__landmark_land_right",
      max("gfw_plantation__type") as "gfw_plantation__type",
      max("is__gfw_mining") as "is__gfw_mining",
      max("is__gfw_managed_forest") as "is__gfw_managed_forest",
      max("is__peatland") as "is__peatland",
      max("idn_forest_area__type") as "idn_forest_area__type",
      max("per_forest_concession__type") as "per_forest_concession__type",
      max("is__gmw_mangroves_2020") as "is__gmw_mangroves_2020",
      max("is__ifl_intact_forest_landscape_2016") as "is__ifl_intact_forest_landscape_2016",
      max("bra_biome__name") as "bra_biome__name",
      max("sbtn_natural_forests__class") as "sbtn_natural_forests__class",
    )

    val aggCols =
      if (!wdpa)
        (max("wdpa_protected_areas__iucn_cat") as "wdpa_protected_areas__iucn_cat") ::
          (max("wdpa_protected_area__iucn_cat") as "wdpa_protected_area__iucn_cat") :: defaultAggCols
      else defaultAggCols

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(aggCols.head, aggCols.tail: _*)
  }
}
