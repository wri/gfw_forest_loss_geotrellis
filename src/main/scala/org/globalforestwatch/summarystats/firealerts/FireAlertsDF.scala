package org.globalforestwatch.summarystats.firealerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object FireAlertsDF {

  val contextualLayers: List[String] = List(
    "is__umd_regional_primary_forest_2001",
    "is__birdlife_alliance_for_zero_extinction_site",
    "is__birdlife_key_biodiversity_area",
    "is__landmark_land_right",
    "gfw_plantation__type",
    "is__gfw_mining",
    "is__gfw_managed_forest",
    "rspo_oil_palm__certification_status",
    "is__gfw_wood_fiber",
    "is__peatland",
    "is__idn_forest_moratorium",
    "is__gfw_oil_palm",
    "idn_forest_area__type",
    "per_forest_concession__type",
    "is__gfw_oil_gas",
    "is__gmw_mangroves_2016",
    "is__ifl_intact_forest_landscape_2016",
    "bra_biome__name"
  )

  def unpackValues(unpackCols: List[Column],
                   wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("fireId", "featureId", "data_group", "data"))

    def defaultCols =
      List(
        $"data_group.primaryForest" as "is__umd_regional_primary_forest_2001",
        $"data_group.aze" as "is__birdlife_alliance_for_zero_extinction_site",
        $"data_group.keyBiodiversityAreas" as "is__birdlife_key_biodiversity_area",
        $"data_group.landmark" as "is__landmark_land_right",
        $"data_group.plantations" as "gfw_plantation__type",
        $"data_group.mining" as "is__gfw_mining",
        $"data_group.logging" as "is__gfw_managed_forest",
        $"data_group.rspo" as "rspo_oil_palm__certification_status",
        $"data_group.woodFiber" as "is__gfw_wood_fiber",
        $"data_group.peatlands" as "is__peatland",
        $"data_group.indonesiaForestMoratorium" as "is__idn_forest_moratorium",
        $"data_group.oilPalm" as "is__gfw_oil_palm",
        $"data_group.indonesiaForestArea" as "idn_forest_area__type",
        $"data_group.peruForestConcessions" as "per_forest_concession__type",
        $"data_group.oilGas" as "is__gfw_oil_gas",
        $"data_group.mangroves2016" as "is__gmw_mangroves_2016",
        $"data_group.intactForestLandscapes2016" as "is__ifl_intact_forest_landscape_2016",
        $"data_group.braBiomes" as "bra_biome__name",
        $"data.totalAlerts" as "alert__count"
      )

    val cols =
      if (!wdpa)
        unpackCols ::: ($"data_group.protectedAreas" as "wdpa_protected_area__iucn_cat") :: defaultCols
      else unpackCols ::: defaultCols

    df.select(cols: _*)
  }

  def aggChangeDaily(groupByCols: List[String],
                     wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val fireCols = List("alert__date", "confidence__cat")
    val cols =
      if (!wdpa)
        groupByCols ::: fireCols ::: "wdpa_protected_area__iucn_cat" :: contextualLayers
      else
        groupByCols ::: fireCols ::: contextualLayers

    df.filter($"alert__date".isNotNull)
      .groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("alert__count") as "alert__count"
      )
  }

  def aggChangeWeekly(cols: List[String],
                      wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val fireCols = List(
      year($"alert__date") as "alert__year",
      weekofyear($"alert__date") as "alert__week",
      $"confidence__cat"
    )
    _aggChangeWeekly(df.filter($"alert__date".isNotNull), cols, fireCols, wdpa)
  }

  def aggChangeWeekly2(cols: List[String],
                       wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val fireCols = List($"alert__year", $"alert__week", $"confidence__cat")
    _aggChangeWeekly(df, cols, fireCols, wdpa)
  }

  private def _aggChangeWeekly(df: DataFrame,
                               cols: List[String],
                               fireCols: List[Column],
                               wdpa: Boolean = false): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val fireCols2 = List("alert__year", "alert__week", "confidence__cat")

    val aggCols = List($"alert__count")

    val contextLayers: List[String] =
      if (!wdpa) "wdpa_protected_area__iucn_cat" :: contextualLayers
      else contextualLayers

    // TODO what the heck is this doing?
    val selectCols: List[Column] = cols.foldRight(Nil: List[Column])(
      col(_) :: _
    ) ::: fireCols ::: contextLayers
      .foldRight(Nil: List[Column])(col(_) :: _) ::: aggCols

    val groupByCols = cols ::: fireCols2 ::: contextLayers

    df.select(selectCols: _*)
      .groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        sum("alert__count") as "alert__count"
      )
  }
}