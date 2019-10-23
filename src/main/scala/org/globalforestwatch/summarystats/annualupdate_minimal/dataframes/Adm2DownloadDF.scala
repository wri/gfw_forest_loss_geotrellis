package org.globalforestwatch.summarystats.annualupdate_minimal.dataframes

import org.apache.spark.sql.functions.{col, round, sum, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Adm2DownloadDF {

  def sumArea(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val year_range = 2001 to 2018

    val annualDF = df
      .groupBy($"iso", $"adm1", $"adm2", $"treecover_density__threshold")
      .pivot("treecover_loss__year", year_range)
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
      .join(annualDF, Seq("iso", "adm1", "adm2", "treecover_density__threshold"), "inner")
      .transform(setNullZero)

  }

  def roundValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._
    df.select(
      $"iso" as "country",
      $"adm1" as "subnational1",
      $"adm2" as "subnational2",
      $"treecover_density__threshold",
      round($"treecover_extent_2000__ha") as "treecover_extent_2000__ha",
      round($"treecover_extent_2010__ha") as "treecover_extent_2010__ha",
      round($"area__ha") as "area__ha",
      round($"treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
      round($"aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
      round($"avg_aboveground_biomass_2000_Mt_ha-1") as "avg_aboveground_biomass_2000_Mt_ha-1",
      round($"aboveground_co2_stock_2000__Mg") as "aboveground_co2_stock_2000__Mg",
      round($"2001_treecover_loss__ha") as "treecover_loss_2001__ha",
      round($"2002_treecover_loss__ha") as "treecover_loss_2002__ha",
      round($"2003_treecover_loss__ha") as "treecover_loss_2003__ha",
      round($"2004_treecover_loss__ha") as "treecover_loss_2004__ha",
      round($"2005_treecover_loss__ha") as "treecover_loss_2005__ha",
      round($"2006_treecover_loss__ha") as "treecover_loss_2006__ha",
      round($"2007_treecover_loss__ha") as "treecover_loss_2007__ha",
      round($"2008_treecover_loss__ha") as "treecover_loss_2008__ha",
      round($"2009_treecover_loss__ha") as "treecover_loss_2009__ha",
      round($"2010_treecover_loss__ha") as "treecover_loss_2010__ha",
      round($"2011_treecover_loss__ha") as "treecover_loss_2011__ha",
      round($"2012_treecover_loss__ha") as "treecover_loss_2012__ha",
      round($"2013_treecover_loss__ha") as "treecover_loss_2013__ha",
      round($"2014_treecover_loss__ha") as "treecover_loss_2014__ha",
      round($"2015_treecover_loss__ha") as "treecover_loss_2015__ha",
      round($"2016_treecover_loss__ha") as "treecover_loss_2016__ha",
      round($"2017_treecover_loss__ha") as "treecover_loss_2017__ha",
      round($"2018_treecover_loss__ha") as "treecover_loss_2018__ha",
      round($"2001_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2001__Mg",
      round($"2002_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2002__Mg",
      round($"2003_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2003__Mg",
      round($"2004_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2004__Mg",
      round($"2005_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2005__Mg",
      round($"2006_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2006__Mg",
      round($"2007_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2007__Mg",
      round($"2008_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2008__Mg",
      round($"2009_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2009__Mg",
      round($"2010_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2010__Mg",
      round($"2011_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2011__Mg",
      round($"2012_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2012__Mg",
      round($"2013_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2013__Mg",
      round($"2014_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2014__Mg",
      round($"2015_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2015__Mg",
      round($"2016_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2016__Mg",
      round($"2017_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2017__Mg",
      round($"2018_aboveground_biomass_loss__Mg") as "aboveground_biomass_loss_2018__Mg",
      round($"2001_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2001__Mg",
      round($"2002_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2002__Mg",
      round($"2003_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2003__Mg",
      round($"2004_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2004__Mg",
      round($"2005_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2005__Mg",
      round($"2006_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2006__Mg",
      round($"2007_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2007__Mg",
      round($"2008_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2008__Mg",
      round($"2009_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2009__Mg",
      round($"2010_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2010__Mg",
      round($"2011_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2011__Mg",
      round($"2012_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2012__Mg",
      round($"2013_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2013__Mg",
      round($"2014_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2014__Mg",
      round($"2015_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2015__Mg",
      round($"2016_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2016__Mg",
      round($"2017_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2017__Mg",
      round($"2018_aboveground_co2_emissions__Mg") as "aboveground_co2_emissions_2018__Mg"
    )
  }

  private def setNullZero(df: DataFrame): DataFrame = {

    def setZero(column: Column): Column =
      when(column.isNull, 0).otherwise(column)

    val nullColumns = df
      .select(
        "2001_treecover_loss__ha",
        "2002_treecover_loss__ha",
        "2003_treecover_loss__ha",
        "2004_treecover_loss__ha",
        "2005_treecover_loss__ha",
        "2006_treecover_loss__ha",
        "2007_treecover_loss__ha",
        "2008_treecover_loss__ha",
        "2009_treecover_loss__ha",
        "2010_treecover_loss__ha",
        "2011_treecover_loss__ha",
        "2012_treecover_loss__ha",
        "2013_treecover_loss__ha",
        "2014_treecover_loss__ha",
        "2015_treecover_loss__ha",
        "2016_treecover_loss__ha",
        "2017_treecover_loss__ha",
        "2018_treecover_loss__ha",
        "2001_aboveground_biomass_loss__Mg",
        "2002_aboveground_biomass_loss__Mg",
        "2003_aboveground_biomass_loss__Mg",
        "2004_aboveground_biomass_loss__Mg",
        "2005_aboveground_biomass_loss__Mg",
        "2006_aboveground_biomass_loss__Mg",
        "2007_aboveground_biomass_loss__Mg",
        "2008_aboveground_biomass_loss__Mg",
        "2009_aboveground_biomass_loss__Mg",
        "2010_aboveground_biomass_loss__Mg",
        "2011_aboveground_biomass_loss__Mg",
        "2012_aboveground_biomass_loss__Mg",
        "2013_aboveground_biomass_loss__Mg",
        "2014_aboveground_biomass_loss__Mg",
        "2015_aboveground_biomass_loss__Mg",
        "2016_aboveground_biomass_loss__Mg",
        "2017_aboveground_biomass_loss__Mg",
        "2018_aboveground_biomass_loss__Mg",
        "2001_aboveground_co2_emissions__Mg",
        "2002_aboveground_co2_emissions__Mg",
        "2003_aboveground_co2_emissions__Mg",
        "2004_aboveground_co2_emissions__Mg",
        "2005_aboveground_co2_emissions__Mg",
        "2006_aboveground_co2_emissions__Mg",
        "2007_aboveground_co2_emissions__Mg",
        "2008_aboveground_co2_emissions__Mg",
        "2009_aboveground_co2_emissions__Mg",
        "2010_aboveground_co2_emissions__Mg",
        "2011_aboveground_co2_emissions__Mg",
        "2012_aboveground_co2_emissions__Mg",
        "2013_aboveground_co2_emissions__Mg",
        "2014_aboveground_co2_emissions__Mg",
        "2015_aboveground_co2_emissions__Mg",
        "2016_aboveground_co2_emissions__Mg",
        "2017_aboveground_co2_emissions__Mg",
        "2018_aboveground_co2_emissions__Mg"
      )
      .columns

    var zeroDF = df

    nullColumns.foreach(column => {
      zeroDF = zeroDF.withColumn(column, setZero(col(column)))
    })

    zeroDF
  }
}
