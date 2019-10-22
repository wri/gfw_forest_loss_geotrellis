package org.globalforestwatch.summarystats.treecoverloss.dataframes

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

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
      $"data_group.tcdYear" as "treecover_extent__year",
      $"data_group.primaryForest" as "is__regional_primary_forest",
      $"data.treecoverExtent2000" as "treecover_extent_2000__ha",
      $"data.treecoverExtent2010" as "treecover_extent_2010__ha",
      $"data.totalArea" as "area__ha",
      $"data.totalGainArea" as "treecover_gain_2000-2012__ha",
      $"data.totalBiomass" as "aboveground_biomass_stock_2000__Mg",
      $"data.avgBiomass" as "avg_aboveground_biomass_stock_2000__Mg_ha-1",
      $"data.totalCo2" as "co2_stock_2000__Mt",
      $"data.lossYear".getItem(0).getItem("treecoverLoss") as "treecover_loss_2001__ha",
      $"data.lossYear".getItem(1).getItem("treecoverLoss") as "treecover_loss_2002__ha",
      $"data.lossYear".getItem(2).getItem("treecoverLoss") as "treecover_loss_2003__ha",
      $"data.lossYear".getItem(3).getItem("treecoverLoss") as "treecover_loss_2004__ha",
      $"data.lossYear".getItem(4).getItem("treecoverLoss") as "treecover_loss_2005__ha",
      $"data.lossYear".getItem(5).getItem("treecoverLoss") as "treecover_loss_2006__ha",
      $"data.lossYear".getItem(6).getItem("treecoverLoss") as "treecover_loss_2007__ha",
      $"data.lossYear".getItem(7).getItem("treecoverLoss") as "treecover_loss_2008__ha",
      $"data.lossYear".getItem(8).getItem("treecoverLoss") as "treecover_loss_2009__ha",
      $"data.lossYear".getItem(9).getItem("treecoverLoss") as "treecover_loss_2010__ha",
      $"data.lossYear".getItem(10).getItem("treecoverLoss") as "treecover_loss_2011__ha",
      $"data.lossYear".getItem(11).getItem("treecoverLoss") as "treecover_loss_2012__ha",
      $"data.lossYear".getItem(12).getItem("treecoverLoss") as "treecover_loss_2013__ha",
      $"data.lossYear".getItem(13).getItem("treecoverLoss") as "treecover_loss_2014__ha",
      $"data.lossYear".getItem(14).getItem("treecoverLoss") as "treecover_loss_2015__ha",
      $"data.lossYear".getItem(15).getItem("treecoverLoss") as "treecover_loss_2016__ha",
      $"data.lossYear".getItem(16).getItem("treecoverLoss") as "treecover_loss_2017__ha",
      $"data.lossYear".getItem(17).getItem("treecoverLoss") as "treecover_loss_2018__ha",
      $"data.lossYear"
        .getItem(0)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2001__Mg",
      $"data.lossYear"
        .getItem(1)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2002__Mg",
      $"data.lossYear"
        .getItem(2)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2003__Mg",
      $"data.lossYear"
        .getItem(3)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2004__Mg",
      $"data.lossYear"
        .getItem(4)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2005__Mg",
      $"data.lossYear"
        .getItem(5)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2006__Mg",
      $"data.lossYear"
        .getItem(6)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2007__Mg",
      $"data.lossYear"
        .getItem(7)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2008__Mg",
      $"data.lossYear"
        .getItem(8)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2009__Mg",
      $"data.lossYear"
        .getItem(9)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2010__Mg",
      $"data.lossYear"
        .getItem(10)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2011__Mg",
      $"data.lossYear"
        .getItem(11)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2012__Mg",
      $"data.lossYear"
        .getItem(12)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2013__Mg",
      $"data.lossYear"
        .getItem(13)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2014__Mg",
      $"data.lossYear"
        .getItem(14)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2015__Mg",
      $"data.lossYear"
        .getItem(15)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2016__Mg",
      $"data.lossYear"
        .getItem(16)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2017__Mg",
      $"data.lossYear"
        .getItem(17)
        .getItem("biomassLoss") as "aboveground_biomass_loss_2018__Mg",
      $"data.lossYear"
        .getItem(0)
        .getItem("carbonEmissions") as "co2_emissions_2001__Mg",
      $"data.lossYear"
        .getItem(1)
        .getItem("carbonEmissions") as "co2_emissions_2002__Mg",
      $"data.lossYear"
        .getItem(2)
        .getItem("carbonEmissions") as "co2_emissions_2003__Mg",
      $"data.lossYear"
        .getItem(3)
        .getItem("carbonEmissions") as "co2_emissions_2004__Mg",
      $"data.lossYear"
        .getItem(4)
        .getItem("carbonEmissions") as "co2_emissions_2005__Mg",
      $"data.lossYear"
        .getItem(5)
        .getItem("carbonEmissions") as "co2_emissions_2006__Mg",
      $"data.lossYear"
        .getItem(6)
        .getItem("carbonEmissions") as "co2_emissions_2007__Mg",
      $"data.lossYear"
        .getItem(7)
        .getItem("carbonEmissions") as "co2_emissions_2008__Mg",
      $"data.lossYear"
        .getItem(8)
        .getItem("carbonEmissions") as "co2_emissions_2009__Mg",
      $"data.lossYear"
        .getItem(9)
        .getItem("carbonEmissions") as "co2_emissions_2010__Mg",
      $"data.lossYear"
        .getItem(10)
        .getItem("carbonEmissions") as "co2_emissions_2011__Mg",
      $"data.lossYear"
        .getItem(11)
        .getItem("carbonEmissions") as "co2_emissions_2012__Mg",
      $"data.lossYear"
        .getItem(12)
        .getItem("carbonEmissions") as "co2_emissions_2013__Mg",
      $"data.lossYear"
        .getItem(13)
        .getItem("carbonEmissions") as "co2_emissions_2014__Mg",
      $"data.lossYear"
        .getItem(14)
        .getItem("carbonEmissions") as "co2_emissions_2015__Mg",
      $"data.lossYear"
        .getItem(15)
        .getItem("carbonEmissions") as "co2_emissions_2016__Mg",
      $"data.lossYear"
        .getItem(16)
        .getItem("carbonEmissions") as "co2_emissions_2017__Mg",
      $"data.lossYear"
        .getItem(17)
        .getItem("carbonEmissions") as "co2_emissions_2018__Mg"
    )
  }

  def primaryForestFilter(include: Boolean)(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    if (include) df
    else {
      df.groupBy($"feature__id", $"treecover_density__threshold", $"treecover_extent__year")
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
