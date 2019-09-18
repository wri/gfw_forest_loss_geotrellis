package org.globalforestwatch.summarystats.annualupdate

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object IsoSummaryDF {

  def sumArea(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.groupBy($"iso", $"threshold")
      .agg(
        sum($"treecover_extent_2000_ha") as "treecover_extent_2000_ha",
        sum($"treecover_extent_2010_ha") as "treecover_extent_2010_ha",
        sum($"total_area_ha") as "total_area_ha",
        sum($"total_gain_2000-2012_ha") as "total_gain_2000-2012_ha",
        sum($"total_biomass_stock_2000_Mt") as "total_biomass_stock_2000_Mt",
        sum($"avg_biomass_2000_Mt_ha-1" * $"treecover_extent_2000_ha") / sum($"treecover_extent_2000_ha") as "avg_biomass_2000_Mt_ha-1",
        sum($"total_co2_stock_Mt") as "total_co2_stock_Mt",
        sum($"area_loss_2001_ha") as "area_loss_2001_ha",
        sum($"area_loss_2002_ha") as "area_loss_2002_ha",
        sum($"area_loss_2003_ha") as "area_loss_2003_ha",
        sum($"area_loss_2004_ha") as "area_loss_2004_ha",
        sum($"area_loss_2005_ha") as "area_loss_2005_ha",
        sum($"area_loss_2006_ha") as "area_loss_2006_ha",
        sum($"area_loss_2007_ha") as "area_loss_2007_ha",
        sum($"area_loss_2008_ha") as "area_loss_2008_ha",
        sum($"area_loss_2009_ha") as "area_loss_2009_ha",
        sum($"area_loss_2010_ha") as "area_loss_2010_ha",
        sum($"area_loss_2011_ha") as "area_loss_2011_ha",
        sum($"area_loss_2012_ha") as "area_loss_2012_ha",
        sum($"area_loss_2013_ha") as "area_loss_2013_ha",
        sum($"area_loss_2014_ha") as "area_loss_2014_ha",
        sum($"area_loss_2015_ha") as "area_loss_2015_ha",
        sum($"area_loss_2016_ha") as "area_loss_2016_ha",
        sum($"area_loss_2017_ha") as "area_loss_2017_ha",
        sum($"area_loss_2018_ha") as "area_loss_2018_ha",
        sum($"biomass_loss_2001_Mt") as "biomass_loss_2001_Mt",
        sum($"biomass_loss_2002_Mt") as "biomass_loss_2002_Mt",
        sum($"biomass_loss_2003_Mt") as "biomass_loss_2003_Mt",
        sum($"biomass_loss_2004_Mt") as "biomass_loss_2004_Mt",
        sum($"biomass_loss_2005_Mt") as "biomass_loss_2005_Mt",
        sum($"biomass_loss_2006_Mt") as "biomass_loss_2006_Mt",
        sum($"biomass_loss_2007_Mt") as "biomass_loss_2007_Mt",
        sum($"biomass_loss_2008_Mt") as "biomass_loss_2008_Mt",
        sum($"biomass_loss_2009_Mt") as "biomass_loss_2009_Mt",
        sum($"biomass_loss_2010_Mt") as "biomass_loss_2010_Mt",
        sum($"biomass_loss_2011_Mt") as "biomass_loss_2011_Mt",
        sum($"biomass_loss_2012_Mt") as "biomass_loss_2012_Mt",
        sum($"biomass_loss_2013_Mt") as "biomass_loss_2013_Mt",
        sum($"biomass_loss_2014_Mt") as "biomass_loss_2014_Mt",
        sum($"biomass_loss_2015_Mt") as "biomass_loss_2015_Mt",
        sum($"biomass_loss_2016_Mt") as "biomass_loss_2016_Mt",
        sum($"biomass_loss_2017_Mt") as "biomass_loss_2017_Mt",
        sum($"biomass_loss_2018_Mt") as "biomass_loss_2018_Mt",
        sum($"co2_emissions_2001_Mt") as "co2_emissions_2001_Mt",
        sum($"co2_emissions_2002_Mt") as "co2_emissions_2002_Mt",
        sum($"co2_emissions_2003_Mt") as "co2_emissions_2003_Mt",
        sum($"co2_emissions_2004_Mt") as "co2_emissions_2004_Mt",
        sum($"co2_emissions_2005_Mt") as "co2_emissions_2005_Mt",
        sum($"co2_emissions_2006_Mt") as "co2_emissions_2006_Mt",
        sum($"co2_emissions_2007_Mt") as "co2_emissions_2007_Mt",
        sum($"co2_emissions_2008_Mt") as "co2_emissions_2008_Mt",
        sum($"co2_emissions_2009_Mt") as "co2_emissions_2009_Mt",
        sum($"co2_emissions_2010_Mt") as "co2_emissions_2010_Mt",
        sum($"co2_emissions_2011_Mt") as "co2_emissions_2011_Mt",
        sum($"co2_emissions_2012_Mt") as "co2_emissions_2012_Mt",
        sum($"co2_emissions_2013_Mt") as "co2_emissions_2013_Mt",
        sum($"co2_emissions_2014_Mt") as "co2_emissions_2014_Mt",
        sum($"co2_emissions_2015_Mt") as "co2_emissions_2015_Mt",
        sum($"co2_emissions_2016_Mt") as "co2_emissions_2016_Mt",
        sum($"co2_emissions_2017_Mt") as "co2_emissions_2017_Mt",
        sum($"co2_emissions_2018_Mt") as "co2_emissions_2018_Mt"
      )
  }

  def roundValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    df.select(
      $"iso" as "country",
      $"threshold",
      round($"treecover_extent_2000_ha") as "treecover_extent_2000_ha",
      round($"treecover_extent_2010_ha") as "treecover_extent_2010_ha",
      round($"total_area_ha") as "total_area_ha",
      round($"total_gain_2000-2012_ha") as "total_gain_2000-2012_ha",
      round($"total_biomass_stock_2000_Mt") as "total_biomass_stock_2000_Mt",
      round($"avg_biomass_2000_Mt_ha-1") as "avg_biomass_2000_Mt_ha-1",
      round($"total_co2_stock_Mt") as "total_co2_stock_Mt",
      round($"area_loss_2001_ha") as "area_loss_2001_ha",
      round($"area_loss_2002_ha") as "area_loss_2002_ha",
      round($"area_loss_2003_ha") as "area_loss_2003_ha",
      round($"area_loss_2004_ha") as "area_loss_2004_ha",
      round($"area_loss_2005_ha") as "area_loss_2005_ha",
      round($"area_loss_2006_ha") as "area_loss_2006_ha",
      round($"area_loss_2007_ha") as "area_loss_2007_ha",
      round($"area_loss_2008_ha") as "area_loss_2008_ha",
      round($"area_loss_2009_ha") as "area_loss_2009_ha",
      round($"area_loss_2010_ha") as "area_loss_2010_ha",
      round($"area_loss_2011_ha") as "area_loss_2011_ha",
      round($"area_loss_2012_ha") as "area_loss_2012_ha",
      round($"area_loss_2013_ha") as "area_loss_2013_ha",
      round($"area_loss_2014_ha") as "area_loss_2014_ha",
      round($"area_loss_2015_ha") as "area_loss_2015_ha",
      round($"area_loss_2016_ha") as "area_loss_2016_ha",
      round($"area_loss_2017_ha") as "area_loss_2017_ha",
      round($"area_loss_2018_ha") as "area_loss_2018_ha",
      round($"biomass_loss_2001_Mt") as "biomass_loss_2001_Mt",
      round($"biomass_loss_2002_Mt") as "biomass_loss_2002_Mt",
      round($"biomass_loss_2003_Mt") as "biomass_loss_2003_Mt",
      round($"biomass_loss_2004_Mt") as "biomass_loss_2004_Mt",
      round($"biomass_loss_2005_Mt") as "biomass_loss_2005_Mt",
      round($"biomass_loss_2006_Mt") as "biomass_loss_2006_Mt",
      round($"biomass_loss_2007_Mt") as "biomass_loss_2007_Mt",
      round($"biomass_loss_2008_Mt") as "biomass_loss_2008_Mt",
      round($"biomass_loss_2009_Mt") as "biomass_loss_2009_Mt",
      round($"biomass_loss_2010_Mt") as "biomass_loss_2010_Mt",
      round($"biomass_loss_2011_Mt") as "biomass_loss_2011_Mt",
      round($"biomass_loss_2012_Mt") as "biomass_loss_2012_Mt",
      round($"biomass_loss_2013_Mt") as "biomass_loss_2013_Mt",
      round($"biomass_loss_2014_Mt") as "biomass_loss_2014_Mt",
      round($"biomass_loss_2015_Mt") as "biomass_loss_2015_Mt",
      round($"biomass_loss_2016_Mt") as "biomass_loss_2016_Mt",
      round($"biomass_loss_2017_Mt") as "biomass_loss_2017_Mt",
      round($"biomass_loss_2018_Mt") as "biomass_loss_2018_Mt",
      round($"co2_emissions_2001_Mt") as "co2_emissions_2001_Mt",
      round($"co2_emissions_2002_Mt") as "co2_emissions_2002_Mt",
      round($"co2_emissions_2003_Mt") as "co2_emissions_2003_Mt",
      round($"co2_emissions_2004_Mt") as "co2_emissions_2004_Mt",
      round($"co2_emissions_2005_Mt") as "co2_emissions_2005_Mt",
      round($"co2_emissions_2006_Mt") as "co2_emissions_2006_Mt",
      round($"co2_emissions_2007_Mt") as "co2_emissions_2007_Mt",
      round($"co2_emissions_2008_Mt") as "co2_emissions_2008_Mt",
      round($"co2_emissions_2009_Mt") as "co2_emissions_2009_Mt",
      round($"co2_emissions_2010_Mt") as "co2_emissions_2010_Mt",
      round($"co2_emissions_2011_Mt") as "co2_emissions_2011_Mt",
      round($"co2_emissions_2012_Mt") as "co2_emissions_2012_Mt",
      round($"co2_emissions_2013_Mt") as "co2_emissions_2013_Mt",
      round($"co2_emissions_2014_Mt") as "co2_emissions_2014_Mt",
      round($"co2_emissions_2015_Mt") as "co2_emissions_2015_Mt",
      round($"co2_emissions_2016_Mt") as "co2_emissions_2016_Mt",
      round($"co2_emissions_2017_Mt") as "co2_emissions_2017_Mt",
      round($"co2_emissions_2018_Mt") as "co2_emissions_2018_Mt"
    )
  }
}
