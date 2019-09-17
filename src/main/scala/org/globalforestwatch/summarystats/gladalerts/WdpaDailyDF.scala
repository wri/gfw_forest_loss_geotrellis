package org.globalforestwatch.summarystats.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WdpaDailyDF {

  def unpackValues(minZoom: Int)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.filter($"data_group.tile.z" === minZoom)
      .select(
        $"id.wdpa_id" as "wdpa_id",
        $"id.name" as "name",
        $"id.iucn_cat" as "iucn_cat",
        $"id.iso" as "iso",
        $"id.status" as "status",
        $"data_group.alertDate" as "alert_date",
        $"data_group.isConfirmed" as "is_confirmed",
        $"data_group.primaryForest" as "primary_forest",
        $"data_group.aze" as "aze",
        $"data_group.keyBiodiversityAreas" as "kba",
        $"data_group.landmark" as "landmark",
        $"data_group.plantations" as "plantations",
        $"data_group.mining" as "mining",
        $"data_group.logging" as "managed_forests",
        $"data_group.rspo" as "rspo",
        $"data_group.woodFiber" as "wood_fiber",
        $"data_group.peatlands" as "peatlands",
        $"data_group.indonesiaForestMoratorium" as "idn_forest_moratorium",
        $"data_group.oilPalm" as "oil_palm",
        $"data_group.indonesiaForestArea" as "idn_forest_area",
        $"data_group.peruForestConcessions" as "per_forest_concession",
        $"data_group.oilGas" as "oil_gas",
        $"data_group.mangroves2016" as "mangroves_2016",
        $"data_group.intactForestLandscapes2016" as "ifl_2016",
        $"data_group.braBiomes" as "bra_biomes",
        $"data.totalAlerts" as "alert_count",
        $"data.alertArea" as "alert_area_ha",
        $"data.co2Emissions" as "co2_emissions_Mt",
        $"data.totalArea" as "total_area_ha"
      )
  }

  def sumAlerts(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "wdpa_id",
        "name",
        "iucn_cat",
        "iso",
        "status",
        "alert_date",
        "is_confirmed",
        "primary_forest",
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
        "co2_emissions_Mt"
      )
    )

    df.filter($"alert_date".isNotNull)
      .groupBy(
        $"wdpa_id",
        $"name",
        $"iucn_cat",
        $"iso",
        $"status",
        $"alert_date",
        $"is_confirmed",
        $"primary_forest",
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
        sum("co2_emissions_Mt") as "co2_emissions_Mt"
      )
  }

  def sumArea(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "wdpa_id",
        "name",
        "iucn_cat",
        "iso",
        "status",
        "primary_forest",
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
        "total_area_ha"
      )
    )

    df.groupBy(
      $"wdpa_id",
      $"name",
      $"iucn_cat",
      $"iso",
      $"status",
      $"primary_forest",
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
      .agg(sum("total_area_ha") as "total_area_ha")
  }
}
