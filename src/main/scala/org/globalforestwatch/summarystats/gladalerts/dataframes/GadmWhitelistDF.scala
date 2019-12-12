package org.globalforestwatch.summarystats.gladalerts.dataframes

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{length, max}

object GadmWhitelistDF {
  def whitelistAdm2(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "iso",
        "adm1",
        "adm2",
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
        "bra_biome__name"
      )
    )

    df.groupBy("iso", "adm1", "adm2")
      .agg(
        max("is__regional_primary_forest") as "is__regional_primary_forest",
        max(length($"wdpa_protected_area__iucn_cat"))
          .cast("boolean") as "wdpa_protected_area__iucn_cat",
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
  }

  def whitelistAdm1(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    validatePresenceOfColumns(
      df,
      Seq(
        "iso",
        "adm1",
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
        "bra_biome__name"
      )
    )

    df.groupBy("iso", "adm1")
      .agg(
        max("is__regional_primary_forest") as "is__regional_primary_forest",
        max("wdpa_protected_area__iucn_cat") as "wdpa_protected_area__iucn_cat",
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
  }

  def whitelistIso(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    validatePresenceOfColumns(
      df,
      Seq(
        "iso",
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
        "bra_biome__name"
      )
    )

    df.groupBy("iso")
      .agg(
        max("is__regional_primary_forest") as "is__regional_primary_forest",
        max("wdpa_protected_area__iucn_cat") as "wdpa_protected_area__iucn_cat",
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
  }
}
