package org.globalforestwatch.summarystats.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SimpleFeatureWeeklyDF {

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
        "intact_forest_landscapes_2016",
        "bra_biome__name",
        "alert__count",
        "alert_area__ha",
        "aboveground_co2_emissions__Mg"
      )
    )

    df.filter($"alert__date".isNotNull)
      .select(
        $"feature__id",
        year($"alert__date") as "alert__year",
        weekofyear($"alert__date") as "alert__week",
        $"is__confirmed_alert",
        $"is__regional_primary_forest",
        $"is__alliance_for_zero_extinction_site",
        $"is__key_biodiversity_area",
        $"is__landmark",
        $"gfw_plantation__type",
        $"is__gfw_mining",
        $"is__gfw_logging",
        $"rspo_oil_palm__certification_status",
        $"is__gfw_wood_fiber",
        $"is__peat_land",
        $"is__idn_forest_moratorium",
        $"is__gfw_oil_palm",
        $"idn_forest_area__type",
        $"per_forest_concession__type",
        $"is__gfw_oil_gas",
        $"is__mangroves_2016",
        $"intact_forest_landscapes_2016",
        $"bra_biome__name",
        $"alert__count",
        $"alert_area__ha",
        $"aboveground_co2_emissions__Mg"
      )
      .groupBy(
        $"feature__id",
        $"alert__year",
        $"alert__week",
        $"is__confirmed_alert",
        $"is__regional_primary_forest",
        $"is__alliance_for_zero_extinction_site",
        $"is__key_biodiversity_area",
        $"is__landmark",
        $"gfw_plantation__type",
        $"is__gfw_mining",
        $"is__gfw_logging",
        $"rspo_oil_palm__certification_status",
        $"is__gfw_wood_fiber",
        $"is__peat_land",
        $"is__idn_forest_moratorium",
        $"is__gfw_oil_palm",
        $"idn_forest_area__type",
        $"per_forest_concession__type",
        $"is__gfw_oil_gas",
        $"is__mangroves_2016",
        $"intact_forest_landscapes_2016",
        $"bra_biome__name"
      )
      .agg(
        sum("alert__count") as "alert__count",
        sum("alert_area__ha") as "alert_area__ha",
        sum("aboveground_co2_emissions__Mg") as "aboveground_co2_emissions__Mg"
      )
  }
}
