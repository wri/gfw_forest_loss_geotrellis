package org.globalforestwatch.summarystats.annualupdate

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm2SummaryDF {

  def sumArea(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val year_range = 2001 to 2018

    val annualDF = df
      .groupBy($"iso", $"adm1", $"adm2", $"threshold")
      .pivot("loss_year", year_range)
      .agg(
        sum("area_loss_ha") as "area_loss_ha",
        sum("biomass_loss_Mt") as "biomass_loss_Mt",
        sum("co2_emissions_Mt") as "co2_emissions_Mt"
      )
      .as("annual")

    val totalDF = df
      .groupBy($"iso", $"adm1", $"adm2", $"threshold")
      .agg(
        sum("treecover_extent_2000_ha") as "treecover_extent_2000_ha",
        sum("treecover_extent_2010_ha") as "treecover_extent_2010_ha",
        sum("total_area_ha") as "total_area_ha",
        sum("total_gain_2000-2012_ha") as "total_gain_2000-2012_ha",
        sum("total_biomass_stock_2000_Mt") as "total_biomass_stock_2000_Mt",
        sum("total_biomass_stock_2000_Mt") / sum("treecover_extent_2000_ha") as "avg_biomass_2000_Mt_ha-1",
        sum("total_co2_stock_Mt") as "total_co2_stock_Mt"
      )
      .as("total")

    totalDF.join(annualDF, Seq("iso", "adm1", "adm2", "threshold"), "inner").transform(setNullZero)

  }

  def roundValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._
    df.select(
      $"iso" as "country",
      $"adm1" as "subnational1",
      $"adm2" as "subnational2",
      $"threshold",
      round($"treecover_extent_2000_ha") as "treecover_extent_2000_ha",
      round($"treecover_extent_2010_ha") as "treecover_extent_2010_ha",
      round($"total_area_ha") as "total_area_ha",
      round($"total_gain_2000-2012_ha") as "total_gain_2000-2012_ha",
      round($"total_biomass_stock_2000_Mt") as "total_biomass_stock_2000_Mt",
      round($"avg_biomass_2000_Mt_ha-1") as "avg_biomass_2000_Mt_ha-1",
      round($"total_co2_stock_Mt") as "total_co2_stock_Mt",
      round($"2001_area_loss_ha") as "area_loss_2001_ha",
      round($"2002_area_loss_ha") as "area_loss_2002_ha",
      round($"2003_area_loss_ha") as "area_loss_2003_ha",
      round($"2004_area_loss_ha") as "area_loss_2004_ha",
      round($"2005_area_loss_ha") as "area_loss_2005_ha",
      round($"2006_area_loss_ha") as "area_loss_2006_ha",
      round($"2007_area_loss_ha") as "area_loss_2007_ha",
      round($"2008_area_loss_ha") as "area_loss_2008_ha",
      round($"2009_area_loss_ha") as "area_loss_2009_ha",
      round($"2010_area_loss_ha") as "area_loss_2010_ha",
      round($"2011_area_loss_ha") as "area_loss_2011_ha",
      round($"2012_area_loss_ha") as "area_loss_2012_ha",
      round($"2013_area_loss_ha") as "area_loss_2013_ha",
      round($"2014_area_loss_ha") as "area_loss_2014_ha",
      round($"2015_area_loss_ha") as "area_loss_2015_ha",
      round($"2016_area_loss_ha") as "area_loss_2016_ha",
      round($"2017_area_loss_ha") as "area_loss_2017_ha",
      round($"2018_area_loss_ha") as "area_loss_2018_ha",
      round($"2001_biomass_loss_Mt") as "biomass_loss_2001_Mt",
      round($"2002_biomass_loss_Mt") as "biomass_loss_2002_Mt",
      round($"2003_biomass_loss_Mt") as "biomass_loss_2003_Mt",
      round($"2004_biomass_loss_Mt") as "biomass_loss_2004_Mt",
      round($"2005_biomass_loss_Mt") as "biomass_loss_2005_Mt",
      round($"2006_biomass_loss_Mt") as "biomass_loss_2006_Mt",
      round($"2007_biomass_loss_Mt") as "biomass_loss_2007_Mt",
      round($"2008_biomass_loss_Mt") as "biomass_loss_2008_Mt",
      round($"2009_biomass_loss_Mt") as "biomass_loss_2009_Mt",
      round($"2010_biomass_loss_Mt") as "biomass_loss_2010_Mt",
      round($"2011_biomass_loss_Mt") as "biomass_loss_2011_Mt",
      round($"2012_biomass_loss_Mt") as "biomass_loss_2012_Mt",
      round($"2013_biomass_loss_Mt") as "biomass_loss_2013_Mt",
      round($"2014_biomass_loss_Mt") as "biomass_loss_2014_Mt",
      round($"2015_biomass_loss_Mt") as "biomass_loss_2015_Mt",
      round($"2016_biomass_loss_Mt") as "biomass_loss_2016_Mt",
      round($"2017_biomass_loss_Mt") as "biomass_loss_2017_Mt",
      round($"2018_biomass_loss_Mt") as "biomass_loss_2018_Mt",
      round($"2001_co2_emissions_Mt") as "co2_emissions_2001_Mt",
      round($"2002_co2_emissions_Mt") as "co2_emissions_2002_Mt",
      round($"2003_co2_emissions_Mt") as "co2_emissions_2003_Mt",
      round($"2004_co2_emissions_Mt") as "co2_emissions_2004_Mt",
      round($"2005_co2_emissions_Mt") as "co2_emissions_2005_Mt",
      round($"2006_co2_emissions_Mt") as "co2_emissions_2006_Mt",
      round($"2007_co2_emissions_Mt") as "co2_emissions_2007_Mt",
      round($"2008_co2_emissions_Mt") as "co2_emissions_2008_Mt",
      round($"2009_co2_emissions_Mt") as "co2_emissions_2009_Mt",
      round($"2010_co2_emissions_Mt") as "co2_emissions_2010_Mt",
      round($"2011_co2_emissions_Mt") as "co2_emissions_2011_Mt",
      round($"2012_co2_emissions_Mt") as "co2_emissions_2012_Mt",
      round($"2013_co2_emissions_Mt") as "co2_emissions_2013_Mt",
      round($"2014_co2_emissions_Mt") as "co2_emissions_2014_Mt",
      round($"2015_co2_emissions_Mt") as "co2_emissions_2015_Mt",
      round($"2016_co2_emissions_Mt") as "co2_emissions_2016_Mt",
      round($"2017_co2_emissions_Mt") as "co2_emissions_2017_Mt",
      round($"2018_co2_emissions_Mt") as "co2_emissions_2018_Mt"
    )
  }

  private def setNullZero(df: DataFrame): DataFrame = {

    def setZero(column: Column): Column = when(column.isNull, 0).otherwise(column)

    val nullColumns = df
      .select(
        "2001_area_loss_ha",
        "2002_area_loss_ha",
        "2003_area_loss_ha",
        "2004_area_loss_ha",
        "2005_area_loss_ha",
        "2006_area_loss_ha",
        "2007_area_loss_ha",
        "2008_area_loss_ha",
        "2009_area_loss_ha",
        "2010_area_loss_ha",
        "2011_area_loss_ha",
        "2012_area_loss_ha",
        "2013_area_loss_ha",
        "2014_area_loss_ha",
        "2015_area_loss_ha",
        "2016_area_loss_ha",
        "2017_area_loss_ha",
        "2018_area_loss_ha",
        "2001_biomass_loss_Mt",
        "2002_biomass_loss_Mt",
        "2003_biomass_loss_Mt",
        "2004_biomass_loss_Mt",
        "2005_biomass_loss_Mt",
        "2006_biomass_loss_Mt",
        "2007_biomass_loss_Mt",
        "2008_biomass_loss_Mt",
        "2009_biomass_loss_Mt",
        "2010_biomass_loss_Mt",
        "2011_biomass_loss_Mt",
        "2012_biomass_loss_Mt",
        "2013_biomass_loss_Mt",
        "2014_biomass_loss_Mt",
        "2015_biomass_loss_Mt",
        "2016_biomass_loss_Mt",
        "2017_biomass_loss_Mt",
        "2018_biomass_loss_Mt",
        "2001_co2_emissions_Mt",
        "2002_co2_emissions_Mt",
        "2003_co2_emissions_Mt",
        "2004_co2_emissions_Mt",
        "2005_co2_emissions_Mt",
        "2006_co2_emissions_Mt",
        "2007_co2_emissions_Mt",
        "2008_co2_emissions_Mt",
        "2009_co2_emissions_Mt",
        "2010_co2_emissions_Mt",
        "2011_co2_emissions_Mt",
        "2012_co2_emissions_Mt",
        "2013_co2_emissions_Mt",
        "2014_co2_emissions_Mt",
        "2015_co2_emissions_Mt",
        "2016_co2_emissions_Mt",
        "2017_co2_emissions_Mt",
        "2018_co2_emissions_Mt"
      )
      .columns

    var zeroDF = df

    nullColumns.foreach(column => {
      zeroDF = zeroDF.withColumn(column, setZero(col(column)))
    })

    zeroDF
  }
}
