package org.globalforestwatch.summarystats.carbonflux_custom_area

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.{length, max, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CarbonCustomDF {

  val contextualLayers: List[String] = List(
    "treecover_density__threshold",
    "carbon_flux_custom_area_1"
  )

  def unpackValues(df: DataFrame): DataFrame = {

    implicit val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "dataGroup", "data"))

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"dataGroup.lossYear" as "treecover_loss__year",
      $"dataGroup.threshold" as "treecover_density__threshold",
      $"dataGroup.carbonFluxCustomArea1" as "carbon_flux_custom_area_1",
      $"data.totalTreecoverLoss" as "treecover_loss__ha",
      $"data.totalBiomassLoss" as "aboveground_biomass_loss__Mg",
      $"data.totalGrossEmissionsCo2eCo2Only" as "gross_emissions_co2e_co2_only__Mg",
      $"data.totalGrossEmissionsCo2eNoneCo2" as "gross_emissions_co2e_non_co2__Mg",
      $"data.totalGrossEmissionsCo2e" as "gross_emissions_co2e_all_gases__Mg",
      $"data.totalTreecoverExtent2000" as "treecover_extent_2000__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
      $"data.totalGrossAnnualRemovalsCarbon" as "gross_annual_biomass_removals_2001-2015__Mg",
      $"data.totalGrossCumulRemovalsCarbon" as "gross_cumulative_co2_removals_2001-2015__Mg",
      $"data.totalNetFluxCo2" as "net_flux_co2_2001-2015__Mg"
    )
  }

  def aggSummary(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("area__ha") as "area__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("gross_annual_biomass_removals_2001-2015__Mg") as "gross_annual_biomass_removals_2001-2015__Mg",
        sum("gross_cumulative_co2_removals_2001-2015__Mg") as "gross_cumulative_co2_removals_2001-2015__Mg",
        sum("net_flux_co2_2001-2015__Mg") as "net_flux_co2_2001-2015__Mg",
        sum("treecover_loss__ha") as "treecover_loss_2001-2015__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2001-2015__Mg",
        sum("gross_emissions_co2e_co2_only__Mg") as "gross_emissions_co2e_co2_only_2001-2015__Mg",
        sum("gross_emissions_co2e_non_co2__Mg") as "gross_emissions_co2e_non_co2_2001-2015__Mg",
        sum("gross_emissions_co2e_all_gases__Mg") as "gross_emissions_co2e_all_gases_2001-2015__Mg"
      )
  }

  def aggSummary2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(groupByCols.head, groupByCols.tail ::: contextualLayers: _*)
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("area__ha") as "area__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("gross_annual_biomass_removals_2001-2015__Mg") as "gross_annual_biomass_removals_2001-2015__Mg",
        sum("gross_cumulative_co2_removals_2001-2015__Mg") as "gross_cumulative_co2_removals_2001-2015__Mg",
        sum("net_flux_co2_2001-2015__Mg") as "net_flux_co2_2001-2015__Mg",
        sum("treecover_loss_2001-2015__ha") as "treecover_loss_2001-2015__ha",
        sum("aboveground_biomass_loss_2001-2015__Mg") as "aboveground_biomass_loss_2001-2015__Mg",
        sum("gross_emissions_co2e_co2_only_2001-2015__Mg") as "gross_emissions_co2e_co2_only_2001-2015__Mg",
        sum("gross_emissions_co2e_non_co2_2001-2015__Mg") as "gross_emissions_co2e_non_co2_2001-2015__Mg",
        sum("gross_emissions_co2e_all_gases_2001-2015__Mg") as "gross_emissions_co2e_all_gases_2001-2015__Mg"
      )
  }

  def aggChange(groupByCols: List[String])(df: DataFrame): DataFrame = {

    df.groupBy(
      groupByCols.head,
      groupByCols.tail ::: List("treecover_loss__year") ::: contextualLayers: _*
    )
      .agg(
        sum("treecover_loss__ha") as "treecover_loss__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss__Mg",
        sum("gross_emissions_co2e_co2_only__Mg") as "gross_emissions_co2e_co2_only__Mg",
        sum("gross_emissions_co2e_non_co2__Mg") as "gross_emissions_co2e_non_co2__Mg",
        sum("gross_emissions_co2e_all_gases__Mg") as "gross_emissions_co2e_all_gases__Mg"
      )
  }

  def whitelist(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        max($"carbon_flux_custom_area_1") as "carbon_flux_custom_area_1"
      )
  }

  def whitelist2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy(groupByCols.head, groupByCols.tail: _*)
      .agg(
        max($"carbon_flux_custom_area_1") as "carbon_flux_custom_area_1"
      )
  }


}
