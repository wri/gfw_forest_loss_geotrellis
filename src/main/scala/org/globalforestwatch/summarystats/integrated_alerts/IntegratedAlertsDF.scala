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
    "is__landmark_indigenous_and_community_lands",
    "is__gfw_peatlands",
    "is__gmw_global_mangrove_extent_2020",
    "is__ifl_intact_forest_landscapes_2016",
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
        $"data_group.landmark" as "is__landmark_indigenous_and_community_lands",
        $"data_group.peatlands" as "is__gfw_peatlands",
        $"data_group.mangroves2020" as "is__gmw_global_mangrove_extent_2020",
        $"data_group.intactForestLandscapes2016" as "is__ifl_intact_forest_landscapes_2016",
        $"data.totalAlerts" as "alert__count",
        $"data.alertArea" as "alert_area__ha",
        $"data.co2Emissions" as "whrc_aboveground_co2_emissions__Mg",
        $"data.totalArea" as "area__ha",
        $"data_group.naturalForests" as "sbtn_natural_forests__class",
      )

    val cols =
      if (!wdpa)
        unpackCols ::: ($"data_group.protectedAreas" as "wdpa_protected_areas__iucn_cat") :: defaultCols
      else unpackCols ::: defaultCols

    df.select(cols: _*)
  }

  def aggChangeDaily(groupByCols: List[String],
                     wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val cols =
      if (!wdpa)
        groupByCols ::: "wdpa_protected_areas__iucn_cat" :: contextualLayers
      else
        groupByCols ::: contextualLayers

    df//.filter($"gfw_integrated_alerts__confidence".notEqual("not_detected"))
      .groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("alert__count") as "alert__count",
        round(sum("alert_area__ha"), 4) as "alert_area__ha",
        round(sum("whrc_aboveground_co2_emissions__Mg"), 4) as "whrc_aboveground_co2_emissions__Mg"
      )
  }

  def aggSummary(groupByCols: List[String],
                 wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val cols =
      if (!wdpa)
        groupByCols ::: "wdpa_protected_areas__iucn_cat" :: contextualLayers
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
      max("is__landmark_indigenous_and_community_lands") as "is__landmark_indigenous_and_community_lands",
      max("is__gfw_peatlands") as "is__gfw_peatlands",
      max("is__gmw_global_mangrove_extent_2020") as "is__gmw_global_mangrove_extent_2020",
      max("is__ifl_intact_forest_landscapes_2016") as "is__ifl_intact_forest_landscapes_2016",
      max(length($"sbtn_natural_forests__class")).cast("boolean") as "sbtn_natural_forests__class"
    )

    val aggCols =
      if (!wdpa)
        (max(length($"wdpa_protected_areas__iucn_cat"))
          .cast("boolean") as "wdpa_protected_areas__iucn_cat") :: defaultAggCols
      else defaultAggCols

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(aggCols.head, aggCols.tail: _*)
  }

  def whitelist2(groupByCols: List[String],
                 wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val defaultAggCols = List(
      max("is__umd_regional_primary_forest_2001") as "is__umd_regional_primary_forest_2001",
      max("is__landmark_indigenous_and_community_lands") as "is__landmark_indigenous_and_community_lands",
      max("is__gfw_peatlands") as "is__gfw_peatlands",
      max("is__gmw_global_mangrove_extent_2020") as "is__gmw_global_mangrove_extent_2020",
      max("is__ifl_intact_forest_landscapes_2016") as "is__ifl_intact_forest_landscapes_2016",
      max("sbtn_natural_forests__class") as "sbtn_natural_forests__class",
    )

    val aggCols =
      if (!wdpa)
        (max("wdpa_protected_areas__iucn_cat") as "wdpa_protected_areas__iucn_cat") :: defaultAggCols
      else defaultAggCols

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(aggCols.head, aggCols.tail: _*)
  }
}
