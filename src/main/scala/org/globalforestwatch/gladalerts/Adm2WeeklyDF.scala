package org.globalforestwatch.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._
import org.globalforestwatch.annualupdate.TreeLossSparkSession

object Adm2WeeklyDF {

  val spark: SparkSession = GladAlertsSparkSession()
  import spark.implicits._

  def sumAlerts(df: DataFrame): DataFrame = {

    validatePresenceOfColumns(
      df,
      Seq(
        "iso",
        "adm1",
        "adm2",
        "alert_date",
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
        "bra_biomes",
        "alert_count",
        "area_ha",
        "co2_emissions_Mt"
      )
    )

    df.select(
        $"iso",
        $"adm1",
        $"adm2",
        year($"alert_date") as "year",
        weekofyear($"alert_date") as "week",
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
      $"ifl_2016",
      $"bra_biomes",
      $"alert_count",
      $"area_ha",
      $"co2_emissions_Mt"
      )
      .groupBy(
        $"iso",
        $"adm1",
        $"adm2",
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
        $"ifl_2016",
        $"bra_biomes"
      )
      .agg(
        sum("alert_count") as "alert_count",
        sum("area_ha") as "area_ha",
        sum("co2_emissions_Mt") as "co2_emissions_Mt"
      )
  }
}
