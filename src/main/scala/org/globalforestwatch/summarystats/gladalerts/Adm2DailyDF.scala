package org.globalforestwatch.summarystats.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm2DailyDF {

  def unpackValues(minZoom: Int = 0)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "id",
        "data_group",
        "data"
      )
    )

    df.filter($"data_group.tile.z" === minZoom)
      .select(
        $"id.iso" as "iso",
        $"id.adm1" as "adm1",
        $"id.adm2" as "adm2",
        $"data_group.alertDate" as "alert_date",
        $"data_group.isConfirmed" as "alert_confirmation_status",
        $"data_group.primaryForest" as "regional_primary_forests",
        $"data_group.protectedAreas" as "wdpa_protected_areas",
        $"data_group.aze" as "alliance_for_zero_extinction_sites",
        $"data_group.keyBiodiversityAreas" as "key_biodiversity_areas",
        $"data_group.landmark" as "landmark",
        $"data_group.plantations" as "gfw_plantations",
        $"data_group.mining" as "gfw_mining",
        $"data_group.logging" as "gfw_logging",
        $"data_group.rspo" as "rspo_oil_palm",
        $"data_group.woodFiber" as "gfw_wood_fiber",
        $"data_group.peatlands" as "peat_lands",
        $"data_group.indonesiaForestMoratorium" as "idn_forest_moratorium",
        $"data_group.oilPalm" as "gfw_oil_palm",
        $"data_group.indonesiaForestArea" as "idn_forest_area",
        $"data_group.peruForestConcessions" as "per_forest_concessions",
        $"data_group.oilGas" as "gfw_oil_gas",
        $"data_group.mangroves2016" as "mangroves_2016",
        $"data_group.intactForestLandscapes2016" as "intact_forest_landscapes_2016",
        $"data_group.braBiomes" as "bra_biomes",
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
        "iso",
        "adm1",
        "adm2",
        "alert_date",
        "alert_confirmation_status",
        "regional_primary_forests",
        "wdpa_protected_areas",
        "alliance_for_zero_extinction_sites",
        "key_biodiversity_areas",
        "landmark",
        "gfw_plantations",
        "gfw_mining",
        "gfw_logging",
        "rspo_oil_palm",
        "gfw_wood_fiber",
        "peat_lands",
        "idn_forest_moratorium",
        "gfw_oil_palm",
        "idn_forest_area",
        "per_forest_concessions",
        "gfw_oil_gas",
        "mangroves_2016",
        "intact_forest_landscapes_2016",
        "bra_biomes",
        "alert_count",
        "alert_area__ha",
        "co2_emissions__Mg"
      )
    )

    df
      .filter($"alert_date".isNotNull)
      .groupBy(
        $"iso",
        $"adm1",
        $"adm2",
        $"alert_date",
        $"alert_confirmation_status",
        $"regional_primary_forests",
        $"wdpa_protected_areas",
        $"alliance_for_zero_extinction_sites",
        $"key_biodiversity_areas",
        $"landmark",
        $"gfw_plantations",
        $"gfw_mining",
        $"gfw_logging",
        $"rspo_oil_palm",
        $"gfw_wood_fiber",
        $"peat_lands",
        $"idn_forest_moratorium",
        $"gfw_oil_palm",
        $"idn_forest_area",
        $"per_forest_concessions",
        $"gfw_oil_gas",
        $"mangroves_2016",
        $"intact_forest_landscapes_2016",
        $"bra_biomes"
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
        "iso",
        "adm1",
        "adm2",
        "regional_primary_forests",
        "wdpa_protected_areas",
        "alliance_for_zero_extinction_sites",
        "key_biodiversity_areas",
        "landmark",
        "gfw_plantations",
        "gfw_mining",
        "gfw_logging",
        "rspo_oil_palm",
        "gfw_wood_fiber",
        "peat_lands",
        "idn_forest_moratorium",
        "gfw_oil_palm",
        "idn_forest_area",
        "per_forest_concessions",
        "gfw_oil_gas",
        "mangroves_2016",
        "intact_forest_landscapes_2016",
        "bra_biomes",
        "area__ha"
      )
    )

    df.groupBy(
      $"iso",
      $"adm1",
      $"adm2",
      $"regional_primary_forests",
      $"wdpa_protected_areas",
      $"alliance_for_zero_extinction_sites",
      $"key_biodiversity_areas",
      $"landmark",
      $"gfw_plantations",
      $"gfw_mining",
      $"gfw_logging",
      $"rspo_oil_palm",
      $"gfw_wood_fiber",
      $"peat_lands",
      $"idn_forest_moratorium",
      $"gfw_oil_palm",
      $"idn_forest_area",
      $"per_forest_concessions",
      $"gfw_oil_gas",
      $"mangroves_2016",
      $"intact_forest_landscapes_2016",
      $"bra_biomes"
    )
      .agg(
        sum("area__ha") as "area__ha"
      )
  }
}
