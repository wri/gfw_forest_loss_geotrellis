package org.globalforestwatch.summarystats.treecoverloss

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions.sum

object TreeLossDF {

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "id",
        "data_group",
        "data"
      )
    )

    df.select(
      $"id.featureId" as "feature__id",
      $"data_group.threshold" as "treecover_density__threshold",
      $"data_group.tcdYear" as "treecover_density__year",
      $"data_group.primaryForest" as "is__regional_primary_forest",
      $"data.treecoverExtent2000" as "treecover_extent_2000__ha",
      $"data.treecoverExtent2010" as "treecover_extent_2010__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalGainArea" as "treecover_gain_2000-2012__ha",
      $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
      $"data.avgBiomass" as "avg_aboveground_biomass_stock_2000__Mg_ha-1",
      $"data.totalCo2" as "co2_stock_2000__Mt",
      $"data.lossYear".getItem(0).getItem("treecover_loss") as "treecover_loss_2001__ha",
      $"data.lossYear".getItem(1).getItem("treecover_loss") as "treecover_loss_2002__ha",
      $"data.lossYear".getItem(2).getItem("treecover_loss") as "treecover_loss_2003__ha",
      $"data.lossYear".getItem(3).getItem("treecover_loss") as "treecover_loss_2004__ha",
      $"data.lossYear".getItem(4).getItem("treecover_loss") as "treecover_loss_2005__ha",
      $"data.lossYear".getItem(5).getItem("treecover_loss") as "treecover_loss_2006__ha",
      $"data.lossYear".getItem(6).getItem("treecover_loss") as "treecover_loss_2007__ha",
      $"data.lossYear".getItem(7).getItem("treecover_loss") as "treecover_loss_2008__ha",
      $"data.lossYear".getItem(8).getItem("treecover_loss") as "treecover_loss_2009__ha",
      $"data.lossYear".getItem(9).getItem("treecover_loss") as "treecover_loss_2010__ha",
      $"data.lossYear".getItem(10).getItem("treecover_loss") as "treecover_loss_2011__ha",
      $"data.lossYear".getItem(11).getItem("treecover_loss") as "treecover_loss_2012__ha",
      $"data.lossYear".getItem(12).getItem("treecover_loss") as "treecover_loss_2013__ha",
      $"data.lossYear".getItem(13).getItem("treecover_loss") as "treecover_loss_2014__ha",
      $"data.lossYear".getItem(14).getItem("treecover_loss") as "treecover_loss_2015__ha",
      $"data.lossYear".getItem(15).getItem("treecover_loss") as "treecover_loss_2016__ha",
      $"data.lossYear".getItem(16).getItem("treecover_loss") as "treecover_loss_2017__ha",
      $"data.lossYear".getItem(17).getItem("treecover_loss") as "treecover_loss_2018__ha",
      $"data.lossYear"
        .getItem(0)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2001__Mg",
      $"data.lossYear"
        .getItem(1)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2002__Mg",
      $"data.lossYear"
        .getItem(2)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2003__Mg",
      $"data.lossYear"
        .getItem(3)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2004__Mg",
      $"data.lossYear"
        .getItem(4)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2005__Mg",
      $"data.lossYear"
        .getItem(5)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2006__Mg",
      $"data.lossYear"
        .getItem(6)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2007__Mg",
      $"data.lossYear"
        .getItem(7)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2008__Mg",
      $"data.lossYear"
        .getItem(8)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2009__Mg",
      $"data.lossYear"
        .getItem(9)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2010__Mg",
      $"data.lossYear"
        .getItem(10)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2011__Mg",
      $"data.lossYear"
        .getItem(11)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2012__Mg",
      $"data.lossYear"
        .getItem(12)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2013__Mg",
      $"data.lossYear"
        .getItem(13)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2014__Mg",
      $"data.lossYear"
        .getItem(14)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2015__Mg",
      $"data.lossYear"
        .getItem(15)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2016__Mg",
      $"data.lossYear"
        .getItem(16)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2017__Mg",
      $"data.lossYear"
        .getItem(17)
        .getItem("biomass_loss") as "aboveground_biomass_loss_2018__Mg",
      $"data.lossYear"
        .getItem(0)
        .getItem("carbon_emissions") as "co2_emissions_2001__Mg",
      $"data.lossYear"
        .getItem(1)
        .getItem("carbon_emissions") as "co2_emissions_2002__Mg",
      $"data.lossYear"
        .getItem(2)
        .getItem("carbon_emissions") as "co2_emissions_2003__Mg",
      $"data.lossYear"
        .getItem(3)
        .getItem("carbon_emissions") as "co2_emissions_2004__Mg",
      $"data.lossYear"
        .getItem(4)
        .getItem("carbon_emissions") as "co2_emissions_2005__Mg",
      $"data.lossYear"
        .getItem(5)
        .getItem("carbon_emissions") as "co2_emissions_2006__Mg",
      $"data.lossYear"
        .getItem(6)
        .getItem("carbon_emissions") as "co2_emissions_2007__Mg",
      $"data.lossYear"
        .getItem(7)
        .getItem("carbon_emissions") as "co2_emissions_2008__Mg",
      $"data.lossYear"
        .getItem(8)
        .getItem("carbon_emissions") as "co2_emissions_2009__Mg",
      $"data.lossYear"
        .getItem(9)
        .getItem("carbon_emissions") as "co2_emissions_2010__Mg",
      $"data.lossYear"
        .getItem(10)
        .getItem("carbon_emissions") as "co2_emissions_2011__Mg",
      $"data.lossYear"
        .getItem(11)
        .getItem("carbon_emissions") as "co2_emissions_2012__Mg",
      $"data.lossYear"
        .getItem(12)
        .getItem("carbon_emissions") as "co2_emissions_2013__Mg",
      $"data.lossYear"
        .getItem(13)
        .getItem("carbon_emissions") as "co2_emissions_2014__Mg",
      $"data.lossYear"
        .getItem(14)
        .getItem("carbon_emissions") as "co2_emissions_2015__Mg",
      $"data.lossYear"
        .getItem(15)
        .getItem("carbon_emissions") as "co2_emissions_2016__Mg",
      $"data.lossYear"
        .getItem(16)
        .getItem("carbon_emissions") as "co2_emissions_2017__Mg",
      $"data.lossYear"
        .getItem(17)
        .getItem("carbon_emissions") as "co2_emissions_2018__Mg"
    )
  }

  def primaryForestFilter(include: Boolean)(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    if (include) df
    else {
      df.groupBy($"feature__id", $"treecover_density__threshold", $"tcd_year")
        .agg(
          sum("area__ha") as "area__ha",
          sum("treecover_extent_2000__ha") as "treecover_extent_2000__ha",
          sum("treecover_extent_2010__ha") as "treecover_extent_2010__ha",
          sum("treecover_gain_2000-2012__ha") as "treecover_gain_2000-2012__ha",
          sum("aboveground_biomass_stock_2000__Mg") as "aboveground_biomass_stock_2000__Mg",
          sum($"avg_aboveground_biomass_stock_2000__Mg_ha-1" * $"treecover_extent_2000__ha") / sum($"treecover_extent_2000__ha") as "avg_aboveground_biomass_stock_2000__Mg_ha-1",
          sum("co2_stock_2000__Mt") as "co2_stock_2000__Mt",
          sum("treecover_loss_2001__ha") as "treecover_loss_2001__ha",
          sum("treecover_loss_2002__ha") as "treecover_loss_2002__ha",
          sum("treecover_loss_2003__ha") as "treecover_loss_2003__ha",
          sum("treecover_loss_2004__ha") as "treecover_loss_2004__ha",
          sum("treecover_loss_2005__ha") as "treecover_loss_2005__ha",
          sum("treecover_loss_2006__ha") as "treecover_loss_2006__ha",
          sum("treecover_loss_2007__ha") as "treecover_loss_2007__ha",
          sum("treecover_loss_2008__ha") as "treecover_loss_2008__ha",
          sum("treecover_loss_2009__ha") as "treecover_loss_2009__ha",
          sum("treecover_loss_2010__ha") as "treecover_loss_2010__ha",
          sum("treecover_loss_2011__ha") as "treecover_loss_2011__ha",
          sum("treecover_loss_2012__ha") as "treecover_loss_2012__ha",
          sum("treecover_loss_2013__ha") as "treecover_loss_2013__ha",
          sum("treecover_loss_2014__ha") as "treecover_loss_2014__ha",
          sum("treecover_loss_2015__ha") as "treecover_loss_2015__ha",
          sum("treecover_loss_2016__ha") as "treecover_loss_2016__ha",
          sum("treecover_loss_2017__ha") as "treecover_loss_2017__ha",
          sum("treecover_loss_2018__ha") as "treecover_loss_2018__ha",
          sum("aboveground_biomass_loss_2001__Mg") as "aboveground_biomass_loss_2001__Mg",
          sum("aboveground_biomass_loss_2002__Mg") as "aboveground_biomass_loss_2002__Mg",
          sum("aboveground_biomass_loss_2003__Mg") as "aboveground_biomass_loss_2003__Mg",
          sum("aboveground_biomass_loss_2004__Mg") as "aboveground_biomass_loss_2004__Mg",
          sum("aboveground_biomass_loss_2005__Mg") as "aboveground_biomass_loss_2005__Mg",
          sum("aboveground_biomass_loss_2006__Mg") as "aboveground_biomass_loss_2006__Mg",
          sum("aboveground_biomass_loss_2007__Mg") as "aboveground_biomass_loss_2007__Mg",
          sum("aboveground_biomass_loss_2008__Mg") as "aboveground_biomass_loss_2008__Mg",
          sum("aboveground_biomass_loss_2009__Mg") as "aboveground_biomass_loss_2009__Mg",
          sum("aboveground_biomass_loss_2010__Mg") as "aboveground_biomass_loss_2010__Mg",
          sum("aboveground_biomass_loss_2011__Mg") as "aboveground_biomass_loss_2011__Mg",
          sum("aboveground_biomass_loss_2012__Mg") as "aboveground_biomass_loss_2012__Mg",
          sum("aboveground_biomass_loss_2013__Mg") as "aboveground_biomass_loss_2013__Mg",
          sum("aboveground_biomass_loss_2014__Mg") as "aboveground_biomass_loss_2014__Mg",
          sum("aboveground_biomass_loss_2015__Mg") as "aboveground_biomass_loss_2015__Mg",
          sum("aboveground_biomass_loss_2016__Mg") as "aboveground_biomass_loss_2016__Mg",
          sum("aboveground_biomass_loss_2017__Mg") as "aboveground_biomass_loss_2017__Mg",
          sum("aboveground_biomass_loss_2018__Mg") as "aboveground_biomass_loss_2018__Mg",
          sum("co2_emissions_2001__Mg") as "co2_emissions_2001__Mg",
          sum("co2_emissions_2002__Mg") as "co2_emissions_2002__Mg",
          sum("co2_emissions_2003__Mg") as "co2_emissions_2003__Mg",
          sum("co2_emissions_2004__Mg") as "co2_emissions_2004__Mg",
          sum("co2_emissions_2005__Mg") as "co2_emissions_2005__Mg",
          sum("co2_emissions_2006__Mg") as "co2_emissions_2006__Mg",
          sum("co2_emissions_2007__Mg") as "co2_emissions_2007__Mg",
          sum("co2_emissions_2008__Mg") as "co2_emissions_2008__Mg",
          sum("co2_emissions_2009__Mg") as "co2_emissions_2009__Mg",
          sum("co2_emissions_2010__Mg") as "co2_emissions_2010__Mg",
          sum("co2_emissions_2011__Mg") as "co2_emissions_2011__Mg",
          sum("co2_emissions_2012__Mg") as "co2_emissions_2012__Mg",
          sum("co2_emissions_2013__Mg") as "co2_emissions_2013__Mg",
          sum("co2_emissions_2014__Mg") as "co2_emissions_2014__Mg",
          sum("co2_emissions_2015__Mg") as "co2_emissions_2015__Mg",
          sum("co2_emissions_2016__Mg") as "co2_emissions_2016__Mg",
          sum("co2_emissions_2017__Mg") as "co2_emissions_2017__Mg",
          sum("co2_emissions_2018__Mg") as "co2_emissions_2018__Mg"
        )
    }
  }

}
