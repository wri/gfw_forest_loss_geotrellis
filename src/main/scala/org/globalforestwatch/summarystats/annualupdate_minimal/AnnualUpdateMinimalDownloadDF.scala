package org.globalforestwatch.summarystats.annualupdate_minimal

import org.apache.spark.sql.functions.{col, round, sum, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object AnnualUpdateMinimalDownloadDF {

  val treecoverLossMinYear = 2001
  val treecoverLossMaxYear = 2018

  def sumDownload(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val yearRange = treecoverLossMinYear to treecoverLossMaxYear

    val annualDF = df
      .groupBy($"iso", $"adm1", $"adm2", $"treecover_density__threshold")
      .pivot("treecover_loss__year", yearRange)
      .agg(
        sum("treecover_loss__ha") as "treecover_loss__ha",
        sum("aboveground_biomass_loss__Mg") as "aboveground_biomass_loss__Mg",
        sum("aboveground_co2_emissions__Mg") as "aboveground_co2_emissions__Mg"
      )
      .as("annual")

    val totalDF = df
      .groupBy($"iso", $"adm1", $"adm2", $"treecover_density__threshold")
      .agg(
        sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
        sum("treecover_extent_2010__ha") as "treecover_extent_2010__ha",
        sum("area__ha") as "area__ha",
        sum("treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
        sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
        sum("aboveground_biomass_stock_2000__Mg") / sum(
          "treecover_extent_2000__ha"
        ) as "avg_aboveground_biomass_2000_Mt_ha-1",
        sum("aboveground_co2_stock_2000__Mg") as "aboveground_co2_stock_2000__Mg"
      )
      .as("total")

    totalDF
      .join(
        annualDF,
        Seq("iso", "adm1", "adm2", "treecover_density__threshold"),
        "inner"
      )
      .transform(setNullZero)

  }

  def sumDownload(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"${i}_treecover_loss__ha") as s"treecover_loss_${i}__ha"
      }).toList

    val abovegroundBiomassCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"${i}_aboveground_biomass_loss__Mg") as s"aboveground_biomass_loss_${i}__Mg"
      }).toList

    val abovegroundCo2EmissionsCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"${i}_aboveground_co2_emissions__Mg") as s"aboveground_co2_emissions_${i}__Mg"

      }).toList
    _sumDownload(
      df,
      groupByCols,
      treecoverLossCols,
      abovegroundBiomassCols,
      abovegroundCo2EmissionsCols
    )
  }

  def sumDownload2(groupByCols: List[String])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"treecover_loss_${i}__ha") as s"treecover_loss_${i}__ha"
      }).toList

    val abovegroundBiomassCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"aboveground_biomass_loss_${i}__Mg") as s"aboveground_biomass_loss_${i}__Mg"
      }).toList

    val abovegroundCo2EmissionsCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        sum($"aboveground_co2_emissions_${i}__Mg") as s"aboveground_co2_emissions_${i}__Mg"

      }).toList

    _sumDownload(
      df,
      groupByCols,
      treecoverLossCols,
      abovegroundBiomassCols,
      abovegroundCo2EmissionsCols
    )
  }

  private def _sumDownload(
                            df: DataFrame,
                            groupByCols: List[String],
                            treecoverLossCols: List[Column],
                            abovegroundBiomassCols: List[Column],
                            abovegroundCo2EmissionsCols: List[Column]
                          ): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._
    val aggCols = List(
      sum($"treecover_extent_2000__ha") as "treecover_extent_2000__ha",
      sum($"treecover_extent_2010__ha") as "treecover_extent_2010__ha",
      sum($"area__ha") as "area__ha",
      sum($"treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
      sum($"aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
      sum($"aboveground_biomass_stock_2000__Mg") / sum(
        $"treecover_extent_2000__ha"
      ) as "avg_aboveground_biomass_2000_Mt_ha-1",
      sum($"aboveground_co2_stock_2000__Mg") as "aboveground_co2_stock_2000__Mg"
    ) ::: treecoverLossCols ::: abovegroundBiomassCols ::: abovegroundCo2EmissionsCols

    df.groupBy(
      groupByCols.head,
      groupByCols.tail ::: List("treecover_density__threshold"): _*
    )
      .agg(aggCols.head, aggCols.tail: _*)
  }

  def roundDownload(roundCols: List[Column])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"${i}_treecover_loss__ha") as s"treecover_loss_${i}__ha"
      }).toList

    val abovegroundBiomassCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"${i}_aboveground_biomass_loss__Mg") as s"aboveground_biomass_loss_${i}__Mg"
      }).toList

    val abovegroundCo2Emissions =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"${i}_aboveground_co2_emissions__Mg") as s"aboveground_co2_emissions_${i}__Mg"
      }).toList

    _roundDownload(
      df,
      roundCols,
      treecoverLossCols,
      abovegroundBiomassCols,
      abovegroundCo2Emissions
    )
  }

  def roundDownload2(roundCols: List[Column])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"treecover_loss_${i}__ha") as s"treecover_loss_${i}__ha"
      }).toList

    val abovegroundBiomassCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"aboveground_biomass_loss_${i}__Mg") as s"aboveground_biomass_loss_${i}__Mg"
      }).toList

    val abovegroundCo2Emissions =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        round($"aboveground_co2_emissions_${i}__Mg") as s"aboveground_co2_emissions_${i}__Mg"
      }).toList

    _roundDownload(
      df,
      roundCols,
      treecoverLossCols,
      abovegroundBiomassCols,
      abovegroundCo2Emissions
    )
  }

  private def _roundDownload(
                              df: DataFrame,
                              roundCols: List[Column],
                              treecoverLossCols: List[Column],
                              abovegroundBiomassCols: List[Column],
                              abovegroundCo2Emissions: List[Column]
                            ): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val cols = List(
      $"treecover_density__threshold",
      round($"treecover_extent_2000__ha") as "treecover_extent_2000__ha",
      round($"treecover_extent_2010__ha") as "treecover_extent_2010__ha",
      round($"area__ha") as "area__ha",
      round($"treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
      round($"aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
      round($"avg_aboveground_biomass_2000_Mt_ha-1") as "avg_aboveground_biomass_2000_Mt_ha-1",
      round($"aboveground_co2_stock_2000__Mg") as "aboveground_co2_stock_2000__Mg"
    )

    df.select(
      roundCols ::: cols ::: treecoverLossCols ::: abovegroundBiomassCols ::: abovegroundCo2Emissions: _*
    )
  }

  private def setNullZero(df: DataFrame): DataFrame = {

    def setZero(column: Column): Column =
      when(column.isNull, 0).otherwise(column)

    val treecoverLossCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        s"${i}_treecover_loss__ha"
      }).toList

    val abovegroundBiomassCols =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        s"${i}_aboveground_biomass_loss__Mg"
      }).toList

    val abovegroundCo2Emissions =
      (for (i <- treecoverLossMinYear to treecoverLossMaxYear) yield {
        s"${i}_aboveground_co2_emissions__Mg"
      }).toList

    val cols = "avg_aboveground_biomass_2000_Mt_ha-1" :: treecoverLossCols ::: abovegroundBiomassCols ::: abovegroundCo2Emissions
    val nullColumns = df
      .select(cols.head, cols.tail: _*)
      .columns

    nullColumns.foldLeft(df)((acc, column) => acc.withColumn(column, setZero(col(column))))

  }
}
