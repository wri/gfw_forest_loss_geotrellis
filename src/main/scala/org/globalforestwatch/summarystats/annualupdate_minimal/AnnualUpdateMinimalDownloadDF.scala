package org.globalforestwatch.summarystats.annualupdate_minimal

import org.apache.spark.sql.functions.{col, round, sum, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object AnnualUpdateMinimalDownloadDF {
  val treecoverLossMinYear = 2001
  val treecoverLossMaxYear = 2021
  val fluxModelTotalYears = (treecoverLossMaxYear - treecoverLossMinYear) + 1

  def sumDownload(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val yearRange = treecoverLossMinYear to treecoverLossMaxYear

    val annualDF = df
      .groupBy($"iso", $"adm1", $"adm2", $"umd_tree_cover_density_2000__threshold")
      .pivot("umd_tree_cover_loss__year", yearRange)
      .agg(
        sum("umd_tree_cover_loss__ha") as "umd_tree_cover_loss__ha",
        sum("gfw_full_extent_gross_emissions__Mg_CO2e") as "gfw_forest_carbon_gross_emissions__Mg_CO2e"
      )
      .as("annual")
      .na.fill(0, Seq("adm1", "adm2"))

    val totalDF = df
      .groupBy($"iso", $"adm1", $"adm2", $"umd_tree_cover_density_2000__threshold")
      .agg(
        sum("umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
        sum("umd_tree_cover_extent_2010__ha") as "umd_tree_cover_extent_2010__ha",
        sum("area__ha") as "area__ha",
        sum("umd_tree_cover_gain__ha") as "umd_tree_cover_gain__ha",
        sum("whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
        sum("whrc_aboveground_biomass_stock_2000__Mg") / sum(
          "umd_tree_cover_extent_2000__ha"
        ) as "avg_whrc_aboveground_biomass_2000_Mg_ha-1",
        sum($"gfw_full_extent_gross_emissions__Mg_CO2e") / fluxModelTotalYears as "gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1",
        sum($"gfw_full_extent_gross_removals__Mg_CO2") / fluxModelTotalYears as "gfw_forest_carbon_gross_removals__Mg_CO2_yr-1",
        sum($"gfw_full_extent_net_flux__Mg_CO2e") / fluxModelTotalYears as "gfw_forest_carbon_net_flux__Mg_CO2e_yr-1"
      )
      .as("total")
      .na.fill(0, Seq("adm1", "adm2"))

    totalDF
      .join(
        annualDF,
        Seq("iso", "adm1", "adm2", "umd_tree_cover_density_2000__threshold"),
        "inner"
      )
      .transform(setNullZero)
      .transform(removeCarbonThresholds)
  }

  def sumDownload(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"${i}_umd_tree_cover_loss__ha") as s"umd_tree_cover_loss_${i}__ha"
      }).toList

    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"${i}_gfw_forest_carbon_gross_emissions__Mg_CO2e") as s"gfw_forest_carbon_gross_emissions_${i}__Mg_CO2e"
      }).toList

    _sumDownload(
      df,
      groupByCols,
      treecoverLossCols,
      totalGrossEmissionsCo2eAllGasesCols
    )
  }

  def sumDownload2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"umd_tree_cover_loss_${i}__ha") as s"umd_tree_cover_loss_${i}__ha"
      }).toList

    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"gfw_forest_carbon_gross_emissions_${i}__Mg_CO2e") as s"gfw_forest_carbon_gross_emissions_${i}__Mg_CO2e"
      }).toList

    _sumDownload(
      df,
      groupByCols,
      treecoverLossCols,
      totalGrossEmissionsCo2eAllGasesCols
    )
  }

  private def _sumDownload(
                            df: DataFrame,
                            groupByCols: List[String],
                            treecoverLossCols: List[Column],
                            totalGrossEmissionsCo2eAllGasesCols: List[Column]
                          ): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._
    val aggCols = List(
      sum($"umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
      sum($"umd_tree_cover_extent_2010__ha") as "umd_tree_cover_extent_2010__ha",
      sum($"area__ha") as "area__ha",
      sum($"umd_tree_cover_gain__ha") as "umd_tree_cover_gain__ha",
      sum($"whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
      sum($"whrc_aboveground_biomass_stock_2000__Mg") / sum(
        $"umd_tree_cover_extent_2000__ha"
      ) as "avg_whrc_aboveground_biomass_2000_Mg_ha-1",
      sum($"gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1") as "gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1",
      sum($"gfw_forest_carbon_gross_removals__Mg_CO2_yr-1") as "gfw_forest_carbon_gross_removals__Mg_CO2_yr-1",
      sum($"gfw_forest_carbon_net_flux__Mg_CO2e_yr-1") as "gfw_forest_carbon_net_flux__Mg_CO2e_yr-1"
    ) ::: treecoverLossCols ::: totalGrossEmissionsCo2eAllGasesCols

    df.groupBy(
      groupByCols.head,
      groupByCols.tail ::: List("umd_tree_cover_density_2000__threshold"): _*
    )
      .agg(aggCols.head, aggCols.tail: _*)
      .na.fill(0, Seq("avg_whrc_aboveground_biomass_2000_Mg_ha-1"))
  }

  def roundDownload(roundCols: List[Column])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"${i}_umd_tree_cover_loss__ha") as s"umd_tree_cover_loss_${i}__ha"
      }).toList

    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"${i}_gfw_forest_carbon_gross_emissions__Mg_CO2e") as s"gfw_forest_carbon_gross_emissions_${i}__Mg_CO2e"
      }).toList

    _roundDownload(
      df,
      roundCols,
      treecoverLossCols,
      totalGrossEmissionsCo2eAllGasesCols
    )
  }

  def roundDownload2(roundCols: List[Column])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"umd_tree_cover_loss_${i}__ha") as s"umd_tree_cover_loss_${i}__ha"
      }).toList

    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"gfw_forest_carbon_gross_emissions_${i}__Mg_CO2e") as s"gfw_forest_carbon_gross_emissions_${i}__Mg_CO2e"
      }).toList

    _roundDownload(
      df,
      roundCols,
      treecoverLossCols,
      totalGrossEmissionsCo2eAllGasesCols
    )
  }

  private def _roundDownload(
                              df: DataFrame,
                              roundCols: List[Column],
                              treecoverLossCols: List[Column],
                              totalGrossEmissionsCo2eAllGasesCols: List[Column],
                            ): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val cols = List(
      $"umd_tree_cover_density_2000__threshold",
      round($"umd_tree_cover_extent_2000__ha") as "umd_tree_cover_extent_2000__ha",
      round($"umd_tree_cover_extent_2010__ha") as "umd_tree_cover_extent_2010__ha",
      round($"area__ha") as "area__ha",
      round($"umd_tree_cover_gain__ha") as "umd_tree_cover_gain__ha",
      round($"whrc_aboveground_biomass_stock_2000__Mg") as "whrc_aboveground_biomass_stock_2000__Mg",
      round($"avg_whrc_aboveground_biomass_2000_Mg_ha-1") as "avg_whrc_aboveground_biomass_2000_Mg_ha-1",
      round($"gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1") as "gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1",
      round($"gfw_forest_carbon_gross_removals__Mg_CO2_yr-1") as "gfw_forest_carbon_gross_removals__Mg_CO2_yr-1",
      round($"gfw_forest_carbon_net_flux__Mg_CO2e_yr-1") as "gfw_forest_carbon_net_flux__Mg_CO2e_yr-1"
    )

    df.select(
      roundCols ::: cols ::: treecoverLossCols ::: totalGrossEmissionsCo2eAllGasesCols : _*
    )
  }

  private def setNullZero(df: DataFrame): DataFrame = {
    def setZero(column: Column): Column =
      when(column.isNull || column.isNaN, 0).otherwise(column)

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        s"${i}_umd_tree_cover_loss__ha"
      }).toList

    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        s"${i}_gfw_forest_carbon_gross_emissions__Mg_CO2e"
      }).toList

    val cols = "avg_whrc_aboveground_biomass_2000_Mg_ha-1" :: "gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1" :: "gfw_forest_carbon_gross_removals__Mg_CO2_yr-1" :: "gfw_forest_carbon_net_flux__Mg_CO2e_yr-1" :: treecoverLossCols ::: totalGrossEmissionsCo2eAllGasesCols
    val nullColumns = df
      .select(cols.head, cols.tail: _*)
      .columns

    nullColumns.foldLeft(df)((acc, column) => acc.withColumn(column, setZero(col(column))))
  }

  /*
    Carbon analysis is only valid for thresholds >=30, so set those columns to null for other thresholds
   */
  private def removeCarbonThresholds(df: DataFrame): DataFrame = {
    def setNull(column: Column): Column =
      when(df("umd_tree_cover_density_2000__threshold") < 30, null).otherwise(column)

    val totalGrossEmissionsCo2eAllGasesCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        s"${i}_gfw_forest_carbon_gross_emissions__Mg_CO2e"
      }).toList

    val cols = "gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1" :: "gfw_forest_carbon_gross_removals__Mg_CO2_yr-1" :: "gfw_forest_carbon_net_flux__Mg_CO2e_yr-1" :: totalGrossEmissionsCo2eAllGasesCols

    val carbonColumns = df
      .select(cols.head, cols.tail: _*)
      .columns

    carbonColumns.foldLeft(df)((acc, column) => acc.withColumn(column, setNull(col(column))))
  }
}
