package org.globalforestwatch.summarystats.gladalerts.dataframes

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum

object GeostoreFeatureDailyDF {

  def unpackValues(minZoom: Int)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.filter($"data_group.tile.z" === minZoom)
      .select(
        $"id.geostoreId" as "geostore__id",
        $"data_group.alertDate" as "alert__date",
        $"data_group.isConfirmed" as "is__confirmed_alert",
        $"data_group.primaryForest" as "is__regional_primary_forest",
        $"data_group.protectedAreas" as "wdpa_protected_area__iucn_cat",
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
  }

  def sumAlerts(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "geostore__id",
        "alert__date",
        "is__confirmed_alert",
        "is__regional_primary_forest",
        "wdpa_protected_area__iucn_cat",
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
        "bra_biome__name",
        "alert__count",
        "alert_area__ha",
        "aboveground_co2_emissions__Mg"
      )
    )

    df.filter($"alert__date".isNotNull)
      .groupBy(
        $"geostore__id",
        $"alert__date",
        $"is__confirmed_alert",
        $"is__regional_primary_forest",
        $"wdpa_protected_area__iucn_cat",
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
        $"is__intact_forest_landscapes_2016",
        $"bra_biome__name"
      )
      .agg(
        sum("alert__count") as "alert__count",
        sum("alert_area__ha") as "alert_area__ha",
        sum("aboveground_co2_emissions__Mg") as "aboveground_co2_emissions__Mg"
      )
  }

  def sumArea(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "geostore__id",
        "is__regional_primary_forest",
        "wdpa_protected_area__iucn_cat",
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
        "bra_biome__name",
        "area__ha"
      )
    )

    df.groupBy(
        $"geostore__id",
        $"is__regional_primary_forest",
      $"wdpa_protected_area__iucn_cat",
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
      $"is__intact_forest_landscapes_2016",
        $"bra_biome__name"
      )
      .agg(sum("area__ha") as "area__ha")
  }
}
