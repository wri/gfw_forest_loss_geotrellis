package org.globalforestwatch.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}
import org.globalforestwatch.treecoverloss.TreeLossSparkSession

object Adm1WeeklyDF {

  val spark: SparkSession = GladAlertsSparkSession()
  import spark.implicits._

  def sumAlerts(df: DataFrame): DataFrame = {

    validatePresenceOfColumns(
      df,
      Seq(
        "iso",
        "adm1",
        "year",
        "week",
        "is_confirmed",
        "primary_forest",
        "wdpa",
        "aze",
        "kba",
        "landmark",
        "plantations",
        "mining",
        "managed_forests",
        "rspo",
        "wood_fiber",
        "peatlands",
        "idn_forest_moratorium",
        "oil_palm",
        "idn_forest_area",
        "per_forest_concession",
        "oil_gas",
        "mangroves_2016",
        "ifl_2016",
        "alert_count",
        "area_ha",
        "co2_emissions_Mt"
      )
    )

    df.groupBy(
      $"iso",
      $"adm1",
      $"year",
      $"week",
      $"is_confirmed",
      $"primary_forest",
      $"wdpa",
      $"aze",
      $"kba",
      $"landmark",
      $"plantations",
      $"mining",
      $"managed_forests",
      $"rspo",
      $"wood_fiber",
      $"peatlands",
      $"idn_forest_moratorium",
      $"oil_palm",
      $"idn_forest_area",
      $"per_forest_concession",
      $"oil_gas",
      $"mangroves_2016",
      $"ifl_2016"
    )
      .agg(
        sum("alert_count") as "alert_count",
        sum("area_ha") as "area_ha",
        sum("co2_emissions_Mt") as "co2_emissions_Mt"
      )
  }
}