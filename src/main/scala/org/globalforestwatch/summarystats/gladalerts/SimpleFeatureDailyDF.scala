package org.globalforestwatch.summarystats.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SimpleFeatureDailyDF {

  def unpackValues(minZoom: Int)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.filter($"data_group.tile.z" === minZoom)
      .select(
        $"id.featureId" as "feature__id",
        $"data_group.alertDate" as "alert__date",
        $"data_group.isConfirmed" as "is__confirmed_alert",
        $"data_group.primaryForest" as "is__regional_primary_forest",
        $"data_group.aze" as "is__alliance_for_zero_extinction_site",
        $"data_group.keyBiodiversityAreas" as "is__key_biodiversity_area",
        $"data_group.landmark" as "is__landmark",
        $"data_group.plantations" as "is__gfw_plantation",
        $"data_group.mining" as "is__gfw_mining",
        $"data_group.logging" as "is__gfw_logging",
        $"data_group.rspo" as "rspo_oil_palm__certification_status",
        $"data_group.woodFiber" as "is__gfw_wood_fiber",
        $"data_group.peatlands" as "is__peat_land",
        $"data_group.indonesiaForestMoratorium" as "is__idn_forest_moratorium",
        $"data_group.oilPalm" as "is__gfw_oil_palm",
        $"data_group.indonesiaForestArea" as "idn_forest_area",
        $"data_group.peruForestConcessions" as "per_forest_concession__type",
        $"data_group.oilGas" as "is__gfw_oil_gas",
        $"data_group.mangroves2016" as "is__mangroves_2016",
        $"data_group.intactForestLandscapes2016" as "intact_forest_landscapes_2016",
        $"data_group.braBiomes" as "bra_biome__name",
        $"data.totalAlerts" as "alert_count",
        $"data.alertArea" as "alert_area__ha",
        $"data.co2Emissions" as "co2_emissions__Mg",
        $"data.totalArea" as "area__ha"
      )
  }

  def sumAlerts(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "feature__id",
        "alert__date",
        "is__confirmed_alert",
        "is__regional_primary_forest",
        "is__alliance_for_zero_extinction_site",
        "is__key_biodiversity_area",
        "is__landmark",
        "is__gfw_plantation",
        "is__gfw_mining",
        "is__gfw_logging",
        "rspo_oil_palm__certification_status",
        "is__gfw_wood_fiber",
        "is__peat_land",
        "is__idn_forest_moratorium",
        "is__gfw_oil_palm",
        "idn_forest_area",
        "per_forest_concession__type",
        "is__gfw_oil_gas",
        "is__mangroves_2016",
        "intact_forest_landscapes_2016",
        "bra_biome__name",
        "alert_count",
        "alert_area__ha",
        "co2_emissions__Mg"
      )
    )

    df.filter($"alert__date".isNotNull)
      .groupBy(
        $"feature__id",
        $"alert__date",
        $"is__confirmed_alert",
        $"is__regional_primary_forest",
        $"is__alliance_for_zero_extinction_site",
        $"is__key_biodiversity_area",
        $"is__landmark",
        $"is__gfw_plantation",
        $"is__gfw_mining",
        $"is__gfw_logging",
        $"rspo_oil_palm__certification_status",
        $"is__gfw_wood_fiber",
        $"is__peat_land",
        $"is__idn_forest_moratorium",
        $"is__gfw_oil_palm",
        $"idn_forest_area",
        $"per_forest_concession__type",
        $"is__gfw_oil_gas",
        $"is__mangroves_2016",
        $"intact_forest_landscapes_2016",
        $"bra_biome__name"
      )
      .agg(
        sum("alert_count") as "alert_count",
        sum("alert_area__ha") as "alert_area__ha",
        sum("co2_emissions__Mg") as "co2_emissions__Mg"
      )
  }

  def sumArea(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "feature__id",
        "is__regional_primary_forest",
        "is__alliance_for_zero_extinction_site",
        "is__key_biodiversity_area",
        "is__landmark",
        "is__gfw_plantation",
        "is__gfw_mining",
        "is__gfw_logging",
        "rspo_oil_palm__certification_status",
        "is__gfw_wood_fiber",
        "is__peat_land",
        "is__idn_forest_moratorium",
        "is__gfw_oil_palm",
        "idn_forest_area",
        "per_forest_concession__type",
        "is__gfw_oil_gas",
        "is__mangroves_2016",
        "intact_forest_landscapes_2016",
        "bra_biome__name",
        "area__ha"
      )
    )

    df.groupBy(
      $"feature__id",
      $"is__regional_primary_forest",
      $"is__alliance_for_zero_extinction_site",
      $"is__key_biodiversity_area",
      $"is__landmark",
      $"is__gfw_plantation",
      $"is__gfw_mining",
      $"is__gfw_logging",
      $"rspo_oil_palm__certification_status",
      $"is__gfw_wood_fiber",
      $"is__peat_land",
      $"is__idn_forest_moratorium",
      $"is__gfw_oil_palm",
        $"idn_forest_area",
      $"per_forest_concession__type",
      $"is__gfw_oil_gas",
      $"is__mangroves_2016",
      $"intact_forest_landscapes_2016",
      $"bra_biome__name"
      )
      .agg(sum("area__ha") as "area__ha")
  }
}
