package org.globalforestwatch.summarystats.annualupdate_minimal

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.{length, max, sum}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object AnnualUpdateMinimalDF {

  val contextualLayers = List(
    "treecover_density__threshold",
    "tcs_driver__type",
    "global_land_cover__class",
    "is__alliance_for_zero_extinction_site",
    "gfw_plantation__type",
    "is__mangroves_1996",
    "is__mangroves_2016",
    "intact_forest_landscape__year",
    "is__tiger_conservation_landscape",
    "is__landmark",
    "is__gfw_land_right",
    "is__key_biodiversity_area",
    "is__gfw_mining",
    "is__peat_land",
    "is__gfw_oil_palm",
    "is__idn_forest_moratorium",
    "is__gfw_wood_fiber",
    "is__gfw_resource_right",
    "is__gfw_logging"
  )

  def unpackValues(cols: List[Column],
                   wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    def defaultUnpackCols =
      List(
        $"data_group.lossYear" as "treecover_loss__year",
        $"data_group.threshold" as "treecover_density__threshold",
        $"data_group.drivers" as "tcs_driver__type",
        $"data_group.globalLandCover" as "global_land_cover__class",
        $"data_group.primaryForest" as "is__regional_primary_forest",
        $"data_group.aze" as "is__alliance_for_zero_extinction_site",
        $"data_group.plantations" as "gfw_plantation__type",
        $"data_group.mangroves1996" as "is__mangroves_1996",
        $"data_group.mangroves2016" as "is__mangroves_2016",
        $"data_group.intactForestLandscapes" as "intact_forest_landscape__year",
        $"data_group.tigerLandscapes" as "is__tiger_conservation_landscape",
        $"data_group.landmark" as "is__landmark",
        $"data_group.landRights" as "is__gfw_land_right",
        $"data_group.keyBiodiversityAreas" as "is__key_biodiversity_area",
        $"data_group.mining" as "is__gfw_mining",
        $"data_group.peatlands" as "is__peat_land",
        $"data_group.oilPalm" as "is__gfw_oil_palm",
        $"data_group.idnForestMoratorium" as "is__idn_forest_moratorium",
        $"data_group.woodFiber" as "is__gfw_wood_fiber",
        $"data_group.resourceRights" as "is__gfw_resource_right",
        $"data_group.logging" as "is__gfw_logging",
        $"data.treecoverExtent2000" as "treecover_extent_2000__ha",
        $"data.treecoverExtent2010" as "treecover_extent_2010__ha",
        $"data.totalArea" as "area__ha",
        $"data.totalGainArea" as "treecover_gain_2000-2012__ha",
        $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
        $"data.totalCo2" as "aboveground_co2_stock_2000__Mg",
        $"data.treecoverLoss" as "treecover_loss__ha",
        $"data.biomassLoss" as "aboveground_biomass_loss__Mg",
        $"data.co2Emissions" as "aboveground_co2_emissions__Mg"
      )

    val unpackCols = {
      if (!wdpa) {
        defaultUnpackCols ::: List(
          $"data_group.wdpa" as "wdpa_protected_area__iucn_cat"
        )
      } else defaultUnpackCols
    }

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.select(cols ::: unpackCols: _*)
  }

  def aggSummary(groupByCols: List[String],
                 wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val cols =
      if (!wdpa)
        groupByCols ::: contextualLayers ::: List(
          "wdpa_protected_area__iucn_cat"
        )
      else groupByCols ::: contextualLayers

    df.groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("treecover_extent_2010__ha") as "treecover_extent_2010__ha",
        sum("area__ha") as "area__ha",
        sum("treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("aboveground_co2_stock_2000__Mg") as "aboveground_co2_stock_2000__Mg",
        sum("treecover_loss__ha") as "treecover_loss_2001-2018__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2001-2018__Mg",
        sum("aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2001-2018__Mg"
      )
  }

  def aggSummary2(groupByCols: List[String],
                  wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val cols =
      if (!wdpa)
        groupByCols ::: contextualLayers ::: List(
          "wdpa_protected_area__iucn_cat"
        )
      else groupByCols ::: contextualLayers

    df.groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("treecover_extent_2010__ha") as "treecover_extent_2010__ha",
        sum("area__ha") as "area__ha",
        sum("treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("aboveground_co2_stock_2000__Mg") as "aboveground_co2_stock_2000__Mg",
        sum("treecover_loss_2001-2018__ha") as "treecover_loss_2001-2018__ha",
        sum("aboveground_biomass_loss_2001-2018__Mg") as "aboveground_biomass_loss_2001-2018__Mg",
        sum("aboveground_co2_emissions_2001-2018__Mg") as "aboveground_co2_emissions_2001-2018__Mg"
      )
  }

  def aggChange(groupByCols: List[String],
                wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val cols =
      if (!wdpa)
        groupByCols ::: List("treecover_loss__year") ::: contextualLayers ::: List(
          "wdpa_protected_area__iucn_cat"
        )
      else groupByCols ::: List("treecover_loss__year") ::: contextualLayers

    df.groupBy(cols.head, cols.tail: _*)
      .agg(
        sum("treecover_loss__ha") as "treecover_loss__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss__Mg",
        sum("aboveground_co2_emissions__Mg") as "aboveground_co2_emissions__Mg"
      )
  }

  def whitelist(groupByCols: List[String],
                wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val defaultAggCols = List(
      max(length($"tcs_driver__type")).cast("boolean") as "tcs_driver__type",
      max(length($"global_land_cover__class"))
        .cast("boolean") as "global_land_cover__class",
      max($"is__regional_primary_forest") as "is__regional_primary_forest",
      max($"is__alliance_for_zero_extinction_site") as "is__alliance_for_zero_extinction_site",
      max(length($"gfw_plantation__type"))
        .cast("boolean") as "gfw_plantation__type",
      max($"is__mangroves_1996") as "is__mangroves_1996",
      max($"is__mangroves_2016") as "is__mangroves_2016",
      max(length($"intact_forest_landscape__year"))
        .cast("boolean") as "intact_forest_landscape__year",
      max($"is__tiger_conservation_landscape") as "is__tiger_conservation_landscape",
      max($"is__landmark") as "is__landmark",
      max($"is__gfw_land_right") as "is__gfw_land_right",
      max($"is__key_biodiversity_area") as "is__key_biodiversity_area",
      max($"is__gfw_mining") as "is__gfw_mining",
      max($"is__peat_land") as "is__peat_land",
      max($"is__gfw_oil_palm") as "is__gfw_oil_palm",
      max($"is__idn_forest_moratorium") as "is__idn_forest_moratorium",
      max($"is__gfw_wood_fiber") as "is__gfw_wood_fiber",
      max($"is__gfw_resource_right") as "is__gfw_resource_right",
      max($"is__gfw_logging") as "is__gfw_logging"
    )

    val aggCols =
      if (!wdpa)
        defaultAggCols ::: List(
          max(length($"wdpa_protected_area__iucn_cat"))
            .cast("boolean") as "wdpa_protected_area__iucn_cat"
        )
      else defaultAggCols

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(aggCols.head, aggCols.tail: _*)

  }

  def whitelist2(groupByCols: List[String],
                 wdpa: Boolean = false)(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    val defaultAggCols: List[Column] = List(
      max($"tcs_driver__type") as "tcs_driver__type",
      max($"global_land_cover__class") as "global_land_cover__class",
      max($"is__regional_primary_forest") as "is__regional_primary_forest",
      max($"is__alliance_for_zero_extinction_site") as "is__alliance_for_zero_extinction_site",
      max($"gfw_plantation__type") as "gfw_plantation__type",
      max($"is__mangroves_1996") as "is__mangroves_1996",
      max($"is__mangroves_2016") as "is__mangroves_2016",
      max($"intact_forest_landscape__year") as "intact_forest_landscape__year",
      max($"is__tiger_conservation_landscape") as "is__tiger_conservation_landscape",
      max($"is__landmark") as "is__landmark",
      max($"is__gfw_land_right") as "is__gfw_land_right",
      max($"is__key_biodiversity_area") as "is__key_biodiversity_area",
      max($"is__gfw_mining") as "is__gfw_mining",
      max($"is__peat_land") as "is__peat_land",
      max($"is__gfw_oil_palm") as "is__gfw_oil_palm",
      max($"is__idn_forest_moratorium") as "is__idn_forest_moratorium",
      max($"is__gfw_wood_fiber") as "is__gfw_wood_fiber",
      max($"is__gfw_resource_right") as "is__gfw_resource_right",
      max($"is__gfw_logging") as "is__gfw_logging"
    )

    val aggCols = if (!wdpa)
      defaultAggCols ::: List(
        max($"wdpa_protected_area__iucn_cat") as "wdpa_protected_area__iucn_cat"
      )
    else defaultAggCols

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(aggCols.head, aggCols.tail: _*)

  }
}
