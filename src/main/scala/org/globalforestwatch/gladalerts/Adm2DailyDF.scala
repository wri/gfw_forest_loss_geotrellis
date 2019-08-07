package org.globalforestwatch.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm2DailyDF {

  def unpackValues(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "id",
        "alert_date",
        "is_confirmed",
        "layers",
        "alert_count",
        "alert_area_ha",
        "co2_emissions_Mt",
        "total_area_ha"
      )
    )

    df.filter($"z" === 0)
      .select(
        $"id.country" as "iso",
        $"id.admin1" as "adm1",
        $"id.admin2" as "adm2",
        $"alert_date",
        $"is_confirmed",
        $"layers.primaryForest" as "primary_forest",
        $"layers.protectedAreas" as "wdpa",
        $"layers.aze" as "aze",
        $"layers.keyBiodiversityAreas" as "kba",
        $"layers.landmark" as "landmark",
        $"layers.plantations" as "plantations",
        $"layers.mining" as "mining",
        $"layers.logging" as "managed_forests",
        $"layers.rspo" as "rspo",
        $"layers.woodFiber" as "wood_fiber",
        $"layers.peatlands" as "peatlands",
        $"layers.indonesiaForestMoratorium" as "idn_forest_moratorium",
        $"layers.oilPalm" as "oil_palm",
        $"layers.indonesiaForestArea" as "idn_forest_area",
        $"layers.peruForestConcessions" as "per_forest_concession",
        $"layers.oilGas" as "oil_gas",
        $"layers.mangroves2016" as "mangroves_2016",
        $"layers.intactForestLandscapes2016" as "ifl_2016",
        $"layers.braBiomes" as "bra_biomes",
        $"alert_count",
        $"alert_area_ha",
        $"co2_emissions_Mt",
        $"total_area_ha"
      )
  }

  def sumAlerts(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

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
        "alert_area_ha",
        "co2_emissions_Mt",
        "total_area_ha"
      )
    )

    df.groupBy(
      $"iso",
      $"adm1",
      $"adm2",
      $"alert_date",
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
        sum("alert_area_ha") as "alert_area_ha",
        sum("co2_emissions_Mt") as "co2_emissions_Mt",
        sum("total_area_ha") as "total_area_ha"
      )
  }
}
