package org.globalforestwatch.summarystats.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object GladAlertsDF {

  val contextualLayers: List[String] = List(
    "is__regional_primary_forest",
    "is__alliance_for_zero_extinction_site",
    "is__key_biodiversity_area",
    "is__landmark",
    "gfw_plantation__type",
    "is__gfw_mining",
    "is__gfw_logging",
    "rspo_oil_palm__certification_status",
    "is__gfw_wood_fiber",
    "is__peat_land",
    "is__idn_forest_moratorium",
    "is__gfw_oil_palm",
    "idn_forest_area__type",
    "per_forest_concession__type",
    "is__gfw_oil_gas",
    "is__mangroves_2016",
    "is__intact_forest_landscapes_2016",
    "bra_biome__name"
  )

  def unpackValues(unpackCols: List[Column],
                   wdpa: Boolean = false,
                   minZoom: Int = 0)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    def defaultCols =
      List(
        $"data_group.alertDate" as "alert__date",
        $"data_group.isConfirmed" as "is__confirmed_alert",
        $"data_group.primaryForest" as "is__regional_primary_forest",
        $"data_group.aze" as "is__alliance_for_zero_extinction_site",
        $"data_group.keyBiodiversityAreas" as "is__key_biodiversity_area",
        $"data_group.landmark" as "is__landmark",
        $"data_group.plantations" as "gfw_plantation__type",
        $"data_group.mining" as "is__gfw_mining",
        $"data_group.logging" as "is__gfw_logging",
        $"data_group.rspo" as "rspo_oil_palm__certification_status",
        $"data_group.woodFiber" as "is__gfw_wood_fiber",
        $"data_group.peatlands" as "is__peat_land",
        $"data_group.indonesiaForestMoratorium" as "is__idn_forest_moratorium",
        $"data_group.oilPalm" as "is__gfw_oil_palm",
        $"data_group.indonesiaForestArea" as "idn_forest_area__type",
        $"data_group.peruForestConcessions" as "per_forest_concession__type",
        $"data_group.oilGas" as "is__gfw_oil_gas",
        $"data_group.mangroves2016" as "is__mangroves_2016",
        $"data_group.intactForestLandscapes2016" as "is__intact_forest_landscapes_2016",
        $"data_group.braBiomes" as "bra_biome__name",
        $"data.totalAlerts" as "alert__count",
        $"data.alertArea" as "alert_area__ha",
        $"data.co2Emissions" as "aboveground_co2_emissions__Mg",
        $"data.totalArea" as "area__ha"
      )

    val cols =
      if (!wdpa)
        unpackCols ::: ($"data_group.protectedAreas" as "wdpa_protected_area__iucn_cat") :: defaultCols
      else unpackCols ::: defaultCols

    df.filter($"data_group.tile.z" === minZoom)
      .select(cols: _*)
  }

  def aggChangeDaily(groupByCols: List[String],
                     wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val gladCols = List("alert__date", "is__confirmed_alert")

    val cols =
      if (!wdpa)
        groupByCols ::: gladCols ::: "wdpa_protected_area__iucn_cat" :: contextualLayers
      else
        groupByCols ::: gladCols ::: contextualLayers

    df.filter($"alert__date".isNotNull)
      .groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("alert__count") as "alert__count",
        sum("alert_area__ha") as "alert_area__ha",
        sum("aboveground_co2_emissions__Mg") as "aboveground_co2_emissions__Mg"
      )
  }

  def aggChangeWeekly(cols: List[String],
                      wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val gladCols = List(
      year($"alert__date") as "alert__year",
      weekofyear($"alert__date") as "alert__week",
      $"is__confirmed_alert"
    )
    aggChangeWeekly(df.filter($"alert__date".isNotNull), cols, gladCols, wdpa)
  }

  def aggChangeWeekly2(cols: List[String],
                       wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val gladCols = List($"alert__year", $"alert__week", $"is__confirmed_alert")
    aggChangeWeekly(df, cols, gladCols, wdpa)
  }

  private def aggChangeWeekly(df: DataFrame,
                              cols: List[String],
                              gladCols: List[Column],
                              wdpa: Boolean = false): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val gladCols2 = List("alert__year", "alert__week", "is__confirmed_alert")

    val aggCols =
      List($"alert__count", $"alert_area__ha", $"aboveground_co2_emissions__Mg")

    val contextLayers: List[String] =
      if (!wdpa) "wdpa_protected_area__iucn_cat" :: contextualLayers
      else contextualLayers

    val selectCols: List[Column] = cols.foldRight(Nil: List[Column])(
      col(_) :: _
    ) ::: gladCols ::: contextLayers
      .foldRight(Nil: List[Column])(col(_) :: _) ::: aggCols

    val groupByCols = cols ::: gladCols2 ::: contextLayers

    df.select(selectCols: _*)
      .groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        sum("alert__count") as "alert__count",
        sum("alert_area__ha") as "alert_area__ha",
        sum("aboveground_co2_emissions__Mg") as "aboveground_co2_emissions__Mg"
      )
  }

  def aggSummary(groupByCols: List[String],
                 wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val cols =
      if (!wdpa)
        groupByCols ::: "wdpa_protected_area__iucn_cat" :: contextualLayers
      else
        groupByCols ::: contextualLayers

    df.groupBy(cols.head, cols.tail: _*)
      .agg(sum("area__ha") as "area__ha")
  }

  def whitelist(groupByCols: List[String],
                wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val _aggCols = List(
      max("is__regional_primary_forest") as "is__regional_primary_forest",
      max("is__alliance_for_zero_extinction_site") as "is__alliance_for_zero_extinction_site",
      max("is__key_biodiversity_area") as "is__key_biodiversity_area",
      max("is__landmark") as "is__landmark",
      max(length($"gfw_plantation__type"))
        .cast("boolean") as "gfw_plantation__type",
      max("is__gfw_mining") as "is__gfw_mining",
      max("is__gfw_logging") as "is__gfw_logging",
      max(length($"rspo_oil_palm__certification_status"))
        .cast("boolean") as "rspo_oil_palm__certification_status",
      max("is__gfw_wood_fiber") as "is__gfw_wood_fiber",
      max("is__peat_land") as "is__peat_land",
      max("is__idn_forest_moratorium") as "is__idn_forest_moratorium",
      max("is__gfw_oil_palm") as "is__gfw_oil_palm",
      max(length($"idn_forest_area__type"))
        .cast("boolean") as "idn_forest_area__type",
      max(length($"per_forest_concession__type"))
        .cast("boolean") as "per_forest_concession__type",
      max("is__gfw_oil_gas") as "is__gfw_oil_gas",
      max("is__mangroves_2016") as "is__mangroves_2016",
      max("is__intact_forest_landscapes_2016") as "is__intact_forest_landscapes_2016",
      max(length($"bra_biome__name")).cast("boolean") as "bra_biome__name"
    )

    val aggCols =
      if (!wdpa)
        (max(length($"wdpa_protected_area__iucn_cat"))
          .cast("boolean") as "wdpa_protected_area__iucn_cat") :: _aggCols
      else _aggCols

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(aggCols.head, aggCols.tail: _*)
  }

  def whitelist2(groupByCols: List[String],
                 wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val _aggCols = List(
      max("is__regional_primary_forest") as "is__regional_primary_forest",
      max("is__alliance_for_zero_extinction_site") as "is__alliance_for_zero_extinction_site",
      max("is__key_biodiversity_area") as "is__key_biodiversity_area",
      max("is__landmark") as "is__landmark",
      max("gfw_plantation__type") as "gfw_plantation__type",
      max("is__gfw_mining") as "is__gfw_mining",
      max("is__gfw_logging") as "is__gfw_logging",
      max("rspo_oil_palm__certification_status") as "rspo_oil_palm__certification_status",
      max("is__gfw_wood_fiber") as "is__gfw_wood_fiber",
      max("is__peat_land") as "is__peat_land",
      max("is__idn_forest_moratorium") as "is__idn_forest_moratorium",
      max("is__gfw_oil_palm") as "is__gfw_oil_palm",
      max("idn_forest_area__type") as "idn_forest_area__type",
      max("per_forest_concession__type") as "per_forest_concession__type",
      max("is__gfw_oil_gas") as "is__gfw_oil_gas",
      max("is__mangroves_2016") as "is__mangroves_2016",
      max("is__intact_forest_landscapes_2016") as "is__intact_forest_landscapes_2016",
      max("bra_biome__name") as "bra_biome__name"
    )

    val aggCols =
      if (!wdpa)
        (max("wdpa_protected_area__iucn_cat") as "wdpa_protected_area__iucn_cat") :: _aggCols
      else _aggCols

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(aggCols.head, aggCols.tail: _*)
  }

}
