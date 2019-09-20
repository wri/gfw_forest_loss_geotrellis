package org.globalforestwatch.summarystats.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WdpaWeeklyDF {

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
        "alert_confirmation_status",
        "regional_primary_forests",
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

    df.filter($"alert_date".isNotNull)
      .select(
        $"wdpa_id",
        $"name",
        $"iucn_cat",
        $"iso",
        $"status",
        year($"alert_date") as "year",
        weekofyear($"alert_date") as "week",
        $"alert_confirmation_status",
        $"regional_primary_forests",
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
        $"bra_biomes",
        $"alert_count",
        $"alert_area__ha",
        $"co2_emissions__Mg"
      )
      .groupBy(
        $"wdpa_id",
        $"name",
        $"iucn_cat",
        $"iso",
        $"status",
        $"year",
        $"week",
        $"alert_confirmation_status",
        $"regional_primary_forests",
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
}
