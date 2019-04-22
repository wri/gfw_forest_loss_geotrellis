package org.globalforestwatch.treecoverloss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.DataFrameHelpers._
import org.globalforestwatch.util.WindowFunctions.windowSum

object AnnualLossDF {

  val spark: SparkSession = TreeLossSparkSession.spark

  import spark.implicits._

  def unpackYearData(df: DataFrame): DataFrame = {

    validatePresenceOfColumns(
      df,
      Seq(
        "feature_id",
        "threshold_2000",
        "layers",
        "area",
        "gain",
        "biomass",
        "biomass_per_ha",
        "co2",
        "mangrove_biomass",
        "mangrove_biomass_per_ha",
        "mangrove_co2",
        "year_data"
      )
    )

    df.select(
      $"feature_id",
      $"threshold_2000" as "threshold",
      $"layers",
      $"area",
      $"gain",
      $"biomass",
      $"biomass_per_ha",
      $"co2",
      $"mangrove_biomass",
      $"mangrove_biomass_per_ha",
      $"mangrove_co2",
      'year_data.getItem(0).getItem("year") as "year_2001",
      'year_data.getItem(1).getItem("year") as "year_2002",
      'year_data.getItem(2).getItem("year") as "year_2003",
      'year_data.getItem(3).getItem("year") as "year_2004",
      'year_data.getItem(4).getItem("year") as "year_2005",
      'year_data.getItem(5).getItem("year") as "year_2006",
      'year_data.getItem(6).getItem("year") as "year_2007",
      'year_data.getItem(7).getItem("year") as "year_2008",
      'year_data.getItem(8).getItem("year") as "year_2009",
      'year_data.getItem(9).getItem("year") as "year_2010",
      'year_data.getItem(10).getItem("year") as "year_2011",
      'year_data.getItem(11).getItem("year") as "year_2012",
      'year_data.getItem(12).getItem("year") as "year_2013",
      'year_data.getItem(13).getItem("year") as "year_2014",
      'year_data.getItem(14).getItem("year") as "year_2015",
      'year_data.getItem(15).getItem("year") as "year_2016",
      'year_data.getItem(16).getItem("year") as "year_2017",
      'year_data.getItem(17).getItem("year") as "year_2018",
      'year_data.getItem(0).getItem("area_loss") as "area_loss_2001",
      'year_data.getItem(1).getItem("area_loss") as "area_loss_2002",
      'year_data.getItem(2).getItem("area_loss") as "area_loss_2003",
      'year_data.getItem(3).getItem("area_loss") as "area_loss_2004",
      'year_data.getItem(4).getItem("area_loss") as "area_loss_2005",
      'year_data.getItem(5).getItem("area_loss") as "area_loss_2006",
      'year_data.getItem(6).getItem("area_loss") as "area_loss_2007",
      'year_data.getItem(7).getItem("area_loss") as "area_loss_2008",
      'year_data.getItem(8).getItem("area_loss") as "area_loss_2009",
      'year_data.getItem(9).getItem("area_loss") as "area_loss_2010",
      'year_data.getItem(10).getItem("area_loss") as "area_loss_2011",
      'year_data.getItem(11).getItem("area_loss") as "area_loss_2012",
      'year_data.getItem(12).getItem("area_loss") as "area_loss_2013",
      'year_data.getItem(13).getItem("area_loss") as "area_loss_2014",
      'year_data.getItem(14).getItem("area_loss") as "area_loss_2015",
      'year_data.getItem(15).getItem("area_loss") as "area_loss_2016",
      'year_data.getItem(16).getItem("area_loss") as "area_loss_2017",
      'year_data.getItem(17).getItem("area_loss") as "area_loss_2018",
      'year_data
        .getItem(0)
        .getItem("biomass_loss") as "biomass_loss_2001",
      'year_data
        .getItem(1)
        .getItem("biomass_loss") as "biomass_loss_2002",
      'year_data
        .getItem(2)
        .getItem("biomass_loss") as "biomass_loss_2003",
      'year_data
        .getItem(3)
        .getItem("biomass_loss") as "biomass_loss_2004",
      'year_data
        .getItem(4)
        .getItem("biomass_loss") as "biomass_loss_2005",
      'year_data
        .getItem(5)
        .getItem("biomass_loss") as "biomass_loss_2006",
      'year_data
        .getItem(6)
        .getItem("biomass_loss") as "biomass_loss_2007",
      'year_data
        .getItem(7)
        .getItem("biomass_loss") as "biomass_loss_2008",
      'year_data
        .getItem(8)
        .getItem("biomass_loss") as "biomass_loss_2009",
      'year_data
        .getItem(9)
        .getItem("biomass_loss") as "biomass_loss_2010",
      'year_data
        .getItem(10)
        .getItem("biomass_loss") as "biomass_loss_2011",
      'year_data
        .getItem(11)
        .getItem("biomass_loss") as "biomass_loss_2012",
      'year_data
        .getItem(12)
        .getItem("biomass_loss") as "biomass_loss_2013",
      'year_data
        .getItem(13)
        .getItem("biomass_loss") as "biomass_loss_2014",
      'year_data
        .getItem(14)
        .getItem("biomass_loss") as "biomass_loss_2015",
      'year_data
        .getItem(15)
        .getItem("biomass_loss") as "biomass_loss_2016",
      'year_data
        .getItem(16)
        .getItem("biomass_loss") as "biomass_loss_2017",
      'year_data
        .getItem(17)
        .getItem("biomass_loss") as "biomass_loss_2018",
      'year_data
        .getItem(0)
        .getItem("carbon_emissions") as "carbon_emissions_2001",
      'year_data
        .getItem(1)
        .getItem("carbon_emissions") as "carbon_emissions_2002",
      'year_data
        .getItem(2)
        .getItem("carbon_emissions") as "carbon_emissions_2003",
      'year_data
        .getItem(3)
        .getItem("carbon_emissions") as "carbon_emissions_2004",
      'year_data
        .getItem(4)
        .getItem("carbon_emissions") as "carbon_emissions_2005",
      'year_data
        .getItem(5)
        .getItem("carbon_emissions") as "carbon_emissions_2006",
      'year_data
        .getItem(6)
        .getItem("carbon_emissions") as "carbon_emissions_2007",
      'year_data
        .getItem(7)
        .getItem("carbon_emissions") as "carbon_emissions_2008",
      'year_data
        .getItem(8)
        .getItem("carbon_emissions") as "carbon_emissions_2009",
      'year_data
        .getItem(9)
        .getItem("carbon_emissions") as "carbon_emissions_2010",
      'year_data
        .getItem(10)
        .getItem("carbon_emissions") as "carbon_emissions_2011",
      'year_data
        .getItem(11)
        .getItem("carbon_emissions") as "carbon_emissions_2012",
      'year_data
        .getItem(12)
        .getItem("carbon_emissions") as "carbon_emissions_2013",
      'year_data
        .getItem(13)
        .getItem("carbon_emissions") as "carbon_emissions_2014",
      'year_data
        .getItem(14)
        .getItem("carbon_emissions") as "carbon_emissions_2015",
      'year_data
        .getItem(15)
        .getItem("carbon_emissions") as "carbon_emissions_2016",
      'year_data
        .getItem(16)
        .getItem("carbon_emissions") as "carbon_emissions_2017",
      'year_data
        .getItem(17)
        .getItem("carbon_emissions") as "carbon_emissions_2018",
      'year_data
        .getItem(0)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2001",
      'year_data
        .getItem(1)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2002",
      'year_data
        .getItem(2)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2003",
      'year_data
        .getItem(3)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2004",
      'year_data
        .getItem(4)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2005",
      'year_data
        .getItem(5)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2006",
      'year_data
        .getItem(6)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2007",
      'year_data
        .getItem(7)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2008",
      'year_data
        .getItem(8)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2009",
      'year_data
        .getItem(9)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2010",
      'year_data
        .getItem(10)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2011",
      'year_data
        .getItem(11)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2012",
      'year_data
        .getItem(12)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2013",
      'year_data
        .getItem(13)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2014",
      'year_data
        .getItem(14)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2015",
      'year_data
        .getItem(15)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2016",
      'year_data
        .getItem(16)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2017",
      'year_data
        .getItem(17)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2018",
      'year_data
        .getItem(0)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2001",
      'year_data
        .getItem(1)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2002",
      'year_data
        .getItem(2)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2003",
      'year_data
        .getItem(3)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2004",
      'year_data
        .getItem(4)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2005",
      'year_data
        .getItem(5)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2006",
      'year_data
        .getItem(6)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2007",
      'year_data
        .getItem(7)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2008",
      'year_data
        .getItem(8)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2009",
      'year_data
        .getItem(9)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2010",
      'year_data
        .getItem(10)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2011",
      'year_data
        .getItem(11)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2012",
      'year_data
        .getItem(12)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2013",
      'year_data
        .getItem(13)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2014",
      'year_data
        .getItem(14)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2015",
      'year_data
        .getItem(15)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2016",
      'year_data
        .getItem(16)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2017",
      'year_data
        .getItem(17)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2018"
    )
  }

  def sumArea(df: DataFrame): DataFrame = {

    df.groupBy(
        $"feature_id",
        $"threshold",
        $"layers",
        $"year_2001",
        $"year_2002",
        $"year_2003",
        $"year_2004",
        $"year_2005",
        $"year_2006",
        $"year_2007",
        $"year_2008",
        $"year_2009",
        $"year_2010",
        $"year_2011",
        $"year_2012",
        $"year_2013",
        $"year_2014",
        $"year_2015",
        $"year_2016",
        $"year_2017",
        $"year_2018"
      )
      .agg(
        sum("area") as "area",
        sum("gain") as "gain",
        sum("biomass") as "biomass",
        sum($"biomass_per_ha" * $"area") / sum("area") as "biomass_per_ha",
        sum("co2") as "co2",
        sum("mangrove_biomass") as "mangrove_biomass",
        sum($"mangrove_biomass_per_ha" * $"area") / sum("area") as "mangrove_biomass_per_ha",
        sum("mangrove_co2") as "mangrove_co2",
        sum("area_loss_2001") as "area_loss_2001",
        sum("area_loss_2002") as "area_loss_2002",
        sum("area_loss_2003") as "area_loss_2003",
        sum("area_loss_2004") as "area_loss_2004",
        sum("area_loss_2005") as "area_loss_2005",
        sum("area_loss_2006") as "area_loss_2006",
        sum("area_loss_2007") as "area_loss_2007",
        sum("area_loss_2008") as "area_loss_2008",
        sum("area_loss_2009") as "area_loss_2009",
        sum("area_loss_2010") as "area_loss_2010",
        sum("area_loss_2011") as "area_loss_2011",
        sum("area_loss_2012") as "area_loss_2012",
        sum("area_loss_2013") as "area_loss_2013",
        sum("area_loss_2014") as "area_loss_2014",
        sum("area_loss_2015") as "area_loss_2015",
        sum("area_loss_2016") as "area_loss_2016",
        sum("area_loss_2017") as "area_loss_2017",
        sum("area_loss_2018") as "area_loss_2018",
        sum("biomass_loss_2001") as "biomass_loss_2001",
        sum("biomass_loss_2002") as "biomass_loss_2002",
        sum("biomass_loss_2003") as "biomass_loss_2003",
        sum("biomass_loss_2004") as "biomass_loss_2004",
        sum("biomass_loss_2005") as "biomass_loss_2005",
        sum("biomass_loss_2006") as "biomass_loss_2006",
        sum("biomass_loss_2007") as "biomass_loss_2007",
        sum("biomass_loss_2008") as "biomass_loss_2008",
        sum("biomass_loss_2009") as "biomass_loss_2009",
        sum("biomass_loss_2010") as "biomass_loss_2010",
        sum("biomass_loss_2011") as "biomass_loss_2011",
        sum("biomass_loss_2012") as "biomass_loss_2012",
        sum("biomass_loss_2013") as "biomass_loss_2013",
        sum("biomass_loss_2014") as "biomass_loss_2014",
        sum("biomass_loss_2015") as "biomass_loss_2015",
        sum("biomass_loss_2016") as "biomass_loss_2016",
        sum("biomass_loss_2017") as "biomass_loss_2017",
        sum("biomass_loss_2018") as "biomass_loss_2018",
        sum("carbon_emissions_2001") as "carbon_emissions_2001",
        sum("carbon_emissions_2002") as "carbon_emissions_2002",
        sum("carbon_emissions_2003") as "carbon_emissions_2003",
        sum("carbon_emissions_2004") as "carbon_emissions_2004",
        sum("carbon_emissions_2005") as "carbon_emissions_2005",
        sum("carbon_emissions_2006") as "carbon_emissions_2006",
        sum("carbon_emissions_2007") as "carbon_emissions_2007",
        sum("carbon_emissions_2008") as "carbon_emissions_2008",
        sum("carbon_emissions_2009") as "carbon_emissions_2009",
        sum("carbon_emissions_2010") as "carbon_emissions_2010",
        sum("carbon_emissions_2011") as "carbon_emissions_2011",
        sum("carbon_emissions_2012") as "carbon_emissions_2012",
        sum("carbon_emissions_2013") as "carbon_emissions_2013",
        sum("carbon_emissions_2014") as "carbon_emissions_2014",
        sum("carbon_emissions_2015") as "carbon_emissions_2015",
        sum("carbon_emissions_2016") as "carbon_emissions_2016",
        sum("carbon_emissions_2017") as "carbon_emissions_2017",
        sum("carbon_emissions_2018") as "carbon_emissions_2018",
        sum("mangrove_biomass_loss_2001") as "mangrove_biomass_loss_2001",
        sum("mangrove_biomass_loss_2002") as "mangrove_biomass_loss_2002",
        sum("mangrove_biomass_loss_2003") as "mangrove_biomass_loss_2003",
        sum("mangrove_biomass_loss_2004") as "mangrove_biomass_loss_2004",
        sum("mangrove_biomass_loss_2005") as "mangrove_biomass_loss_2005",
        sum("mangrove_biomass_loss_2006") as "mangrove_biomass_loss_2006",
        sum("mangrove_biomass_loss_2007") as "mangrove_biomass_loss_2007",
        sum("mangrove_biomass_loss_2008") as "mangrove_biomass_loss_2008",
        sum("mangrove_biomass_loss_2009") as "mangrove_biomass_loss_2009",
        sum("mangrove_biomass_loss_2010") as "mangrove_biomass_loss_2010",
        sum("mangrove_biomass_loss_2011") as "mangrove_biomass_loss_2011",
        sum("mangrove_biomass_loss_2012") as "mangrove_biomass_loss_2012",
        sum("mangrove_biomass_loss_2013") as "mangrove_biomass_loss_2013",
        sum("mangrove_biomass_loss_2014") as "mangrove_biomass_loss_2014",
        sum("mangrove_biomass_loss_2015") as "mangrove_biomass_loss_2015",
        sum("mangrove_biomass_loss_2016") as "mangrove_biomass_loss_2016",
        sum("mangrove_biomass_loss_2017") as "mangrove_biomass_loss_2017",
        sum("mangrove_biomass_loss_2018") as "mangrove_biomass_loss_2018",
        sum("mangrove_carbon_emissions_2001") as "mangrove_carbon_emissions_2001",
        sum("mangrove_carbon_emissions_2002") as "mangrove_carbon_emissions_2002",
        sum("mangrove_carbon_emissions_2003") as "mangrove_carbon_emissions_2003",
        sum("mangrove_carbon_emissions_2004") as "mangrove_carbon_emissions_2004",
        sum("mangrove_carbon_emissions_2005") as "mangrove_carbon_emissions_2005",
        sum("mangrove_carbon_emissions_2006") as "mangrove_carbon_emissions_2006",
        sum("mangrove_carbon_emissions_2007") as "mangrove_carbon_emissions_2007",
        sum("mangrove_carbon_emissions_2008") as "mangrove_carbon_emissions_2008",
        sum("mangrove_carbon_emissions_2009") as "mangrove_carbon_emissions_2009",
        sum("mangrove_carbon_emissions_2010") as "mangrove_carbon_emissions_2010",
        sum("mangrove_carbon_emissions_2011") as "mangrove_carbon_emissions_2011",
        sum("mangrove_carbon_emissions_2012") as "mangrove_carbon_emissions_2012",
        sum("mangrove_carbon_emissions_2013") as "mangrove_carbon_emissions_2013",
        sum("mangrove_carbon_emissions_2014") as "mangrove_carbon_emissions_2014",
        sum("mangrove_carbon_emissions_2015") as "mangrove_carbon_emissions_2015",
        sum("mangrove_carbon_emissions_2016") as "mangrove_carbon_emissions_2016",
        sum("mangrove_carbon_emissions_2017") as "mangrove_carbon_emissions_2017",
        sum("mangrove_carbon_emissions_2018") as "mangrove_carbon_emissions_2018"
      )
  }

  def joinMaster(masterDF: DataFrame)(df: DataFrame): DataFrame = {

    df.join(
        masterDF,
        $"feature_id" <=> $"m_feature_id"
          && $"layers" <=> $"m_layers" && $"threshold" <=> $"m_threshold",
        "right_outer"
      )
      .na
      .fill(
        0.0,
        Seq(
          "area",
          "gain",
          "biomass",
          "biomass_per_ha",
          "co2",
          "mangrove_biomass",
          "mangrove_biomass_per_ha",
          "mangrove_co2",
          "area_loss_2001",
          "area_loss_2002",
          "area_loss_2003",
          "area_loss_2004",
          "area_loss_2005",
          "area_loss_2006",
          "area_loss_2007",
          "area_loss_2008",
          "area_loss_2009",
          "area_loss_2010",
          "area_loss_2011",
          "area_loss_2012",
          "area_loss_2013",
          "area_loss_2014",
          "area_loss_2015",
          "area_loss_2016",
          "area_loss_2017",
          "area_loss_2018",
          "biomass_loss_2001",
          "biomass_loss_2002",
          "biomass_loss_2003",
          "biomass_loss_2004",
          "biomass_loss_2005",
          "biomass_loss_2006",
          "biomass_loss_2007",
          "biomass_loss_2008",
          "biomass_loss_2009",
          "biomass_loss_2010",
          "biomass_loss_2011",
          "biomass_loss_2012",
          "biomass_loss_2013",
          "biomass_loss_2014",
          "biomass_loss_2015",
          "biomass_loss_2016",
          "biomass_loss_2017",
          "biomass_loss_2018",
          "carbon_emissions_2001",
          "carbon_emissions_2002",
          "carbon_emissions_2003",
          "carbon_emissions_2004",
          "carbon_emissions_2005",
          "carbon_emissions_2006",
          "carbon_emissions_2007",
          "carbon_emissions_2008",
          "carbon_emissions_2009",
          "carbon_emissions_2010",
          "carbon_emissions_2011",
          "carbon_emissions_2012",
          "carbon_emissions_2013",
          "carbon_emissions_2014",
          "carbon_emissions_2015",
          "carbon_emissions_2016",
          "carbon_emissions_2017",
          "carbon_emissions_2018",
          "mangrove_biomass_loss_2001",
          "mangrove_biomass_loss_2002",
          "mangrove_biomass_loss_2003",
          "mangrove_biomass_loss_2004",
          "mangrove_biomass_loss_2005",
          "mangrove_biomass_loss_2006",
          "mangrove_biomass_loss_2007",
          "mangrove_biomass_loss_2008",
          "mangrove_biomass_loss_2009",
          "mangrove_biomass_loss_2010",
          "mangrove_biomass_loss_2011",
          "mangrove_biomass_loss_2012",
          "mangrove_biomass_loss_2013",
          "mangrove_biomass_loss_2014",
          "mangrove_biomass_loss_2015",
          "mangrove_biomass_loss_2016",
          "mangrove_biomass_loss_2017",
          "mangrove_biomass_loss_2018",
          "mangrove_carbon_emissions_2001",
          "mangrove_carbon_emissions_2002",
          "mangrove_carbon_emissions_2003",
          "mangrove_carbon_emissions_2004",
          "mangrove_carbon_emissions_2005",
          "mangrove_carbon_emissions_2006",
          "mangrove_carbon_emissions_2007",
          "mangrove_carbon_emissions_2008",
          "mangrove_carbon_emissions_2009",
          "mangrove_carbon_emissions_2010",
          "mangrove_carbon_emissions_2011",
          "mangrove_carbon_emissions_2012",
          "mangrove_carbon_emissions_2013",
          "mangrove_carbon_emissions_2014",
          "mangrove_carbon_emissions_2015",
          "mangrove_carbon_emissions_2016",
          "mangrove_carbon_emissions_2017",
          "mangrove_carbon_emissions_2018"
        )
      )
      .na
      .fill(2001, Seq("year_2001"))
      .na
      .fill(2002, Seq("year_2002"))
      .na
      .fill(2003, Seq("year_2003"))
      .na
      .fill(2004, Seq("year_2004"))
      .na
      .fill(2005, Seq("year_2005"))
      .na
      .fill(2006, Seq("year_2006"))
      .na
      .fill(2007, Seq("year_2007"))
      .na
      .fill(2008, Seq("year_2008"))
      .na
      .fill(2009, Seq("year_2009"))
      .na
      .fill(2010, Seq("year_2010"))
      .na
      .fill(2011, Seq("year_2011"))
      .na
      .fill(2012, Seq("year_2012"))
      .na
      .fill(2013, Seq("year_2013"))
      .na
      .fill(2014, Seq("year_2014"))
      .na
      .fill(2015, Seq("year_2015"))
      .na
      .fill(2016, Seq("year_2016"))
      .na
      .fill(2017, Seq("year_2017"))
      .na
      .fill(2018, Seq("year_2018"))
  }

  def aggregateByThreshold(df: DataFrame): DataFrame = {

    df.select(
      $"m_feature_id" as "feature_id",
      $"m_threshold" as "threshold",
      $"m_layers" as "layers",
      $"totalarea",
      windowSum("area") as "extent2000",
      windowSum("gain") as "total_gain",
      windowSum("biomass") as "total_biomass",
      windowSum($"biomass_per_ha" * $"area") / windowSum("area") as "avg_biomass_per_ha",
      windowSum("co2") as "total_co2",
      windowSum("mangrove_biomass") as "total_mangrove_biomass",
      windowSum("mangrove_co2") as "total_mangrove_co2",
      windowSum($"mangrove_biomass_per_ha" * $"area") / windowSum("area") as "avg_mangrove_biomass_per_ha",
      $"year_2001",
      $"year_2002",
      $"year_2003",
      $"year_2004",
      $"year_2005",
      $"year_2006",
      $"year_2007",
      $"year_2008",
      $"year_2009",
      $"year_2010",
      $"year_2011",
      $"year_2012",
      $"year_2013",
      $"year_2014",
      $"year_2015",
      $"year_2016",
      $"year_2017",
      $"year_2018",
      windowSum("area_loss_2001") as "area_loss_2001",
      windowSum("area_loss_2002") as "area_loss_2002",
      windowSum("area_loss_2003") as "area_loss_2003",
      windowSum("area_loss_2004") as "area_loss_2004",
      windowSum("area_loss_2005") as "area_loss_2005",
      windowSum("area_loss_2006") as "area_loss_2006",
      windowSum("area_loss_2007") as "area_loss_2007",
      windowSum("area_loss_2008") as "area_loss_2008",
      windowSum("area_loss_2009") as "area_loss_2009",
      windowSum("area_loss_2010") as "area_loss_2010",
      windowSum("area_loss_2011") as "area_loss_2011",
      windowSum("area_loss_2012") as "area_loss_2012",
      windowSum("area_loss_2013") as "area_loss_2013",
      windowSum("area_loss_2014") as "area_loss_2014",
      windowSum("area_loss_2015") as "area_loss_2015",
      windowSum("area_loss_2016") as "area_loss_2016",
      windowSum("area_loss_2017") as "area_loss_2017",
      windowSum("area_loss_2018") as "area_loss_2018",
      windowSum("biomass_loss_2001") as "biomass_loss_2001",
      windowSum("biomass_loss_2002") as "biomass_loss_2002",
      windowSum("biomass_loss_2003") as "biomass_loss_2003",
      windowSum("biomass_loss_2004") as "biomass_loss_2004",
      windowSum("biomass_loss_2005") as "biomass_loss_2005",
      windowSum("biomass_loss_2006") as "biomass_loss_2006",
      windowSum("biomass_loss_2007") as "biomass_loss_2007",
      windowSum("biomass_loss_2008") as "biomass_loss_2008",
      windowSum("biomass_loss_2009") as "biomass_loss_2009",
      windowSum("biomass_loss_2010") as "biomass_loss_2010",
      windowSum("biomass_loss_2011") as "biomass_loss_2011",
      windowSum("biomass_loss_2012") as "biomass_loss_2012",
      windowSum("biomass_loss_2013") as "biomass_loss_2013",
      windowSum("biomass_loss_2014") as "biomass_loss_2014",
      windowSum("biomass_loss_2015") as "biomass_loss_2015",
      windowSum("biomass_loss_2016") as "biomass_loss_2016",
      windowSum("biomass_loss_2017") as "biomass_loss_2017",
      windowSum("biomass_loss_2018") as "biomass_loss_2018",
      windowSum("carbon_emissions_2001") as "carbon_emissions_2001",
      windowSum("carbon_emissions_2002") as "carbon_emissions_2002",
      windowSum("carbon_emissions_2003") as "carbon_emissions_2003",
      windowSum("carbon_emissions_2004") as "carbon_emissions_2004",
      windowSum("carbon_emissions_2005") as "carbon_emissions_2005",
      windowSum("carbon_emissions_2006") as "carbon_emissions_2006",
      windowSum("carbon_emissions_2007") as "carbon_emissions_2007",
      windowSum("carbon_emissions_2008") as "carbon_emissions_2008",
      windowSum("carbon_emissions_2009") as "carbon_emissions_2009",
      windowSum("carbon_emissions_2010") as "carbon_emissions_2010",
      windowSum("carbon_emissions_2011") as "carbon_emissions_2011",
      windowSum("carbon_emissions_2012") as "carbon_emissions_2012",
      windowSum("carbon_emissions_2013") as "carbon_emissions_2013",
      windowSum("carbon_emissions_2014") as "carbon_emissions_2014",
      windowSum("carbon_emissions_2015") as "carbon_emissions_2015",
      windowSum("carbon_emissions_2016") as "carbon_emissions_2016",
      windowSum("carbon_emissions_2017") as "carbon_emissions_2017",
      windowSum("carbon_emissions_2018") as "carbon_emissions_2018",
      windowSum("mangrove_biomass_loss_2001") as "mangrove_biomass_loss_2001",
      windowSum("mangrove_biomass_loss_2002") as "mangrove_biomass_loss_2002",
      windowSum("mangrove_biomass_loss_2003") as "mangrove_biomass_loss_2003",
      windowSum("mangrove_biomass_loss_2004") as "mangrove_biomass_loss_2004",
      windowSum("mangrove_biomass_loss_2005") as "mangrove_biomass_loss_2005",
      windowSum("mangrove_biomass_loss_2006") as "mangrove_biomass_loss_2006",
      windowSum("mangrove_biomass_loss_2007") as "mangrove_biomass_loss_2007",
      windowSum("mangrove_biomass_loss_2008") as "mangrove_biomass_loss_2008",
      windowSum("mangrove_biomass_loss_2009") as "mangrove_biomass_loss_2009",
      windowSum("mangrove_biomass_loss_2010") as "mangrove_biomass_loss_2010",
      windowSum("mangrove_biomass_loss_2011") as "mangrove_biomass_loss_2011",
      windowSum("mangrove_biomass_loss_2012") as "mangrove_biomass_loss_2012",
      windowSum("mangrove_biomass_loss_2013") as "mangrove_biomass_loss_2013",
      windowSum("mangrove_biomass_loss_2014") as "mangrove_biomass_loss_2014",
      windowSum("mangrove_biomass_loss_2015") as "mangrove_biomass_loss_2015",
      windowSum("mangrove_biomass_loss_2016") as "mangrove_biomass_loss_2016",
      windowSum("mangrove_biomass_loss_2017") as "mangrove_biomass_loss_2017",
      windowSum("mangrove_biomass_loss_2018") as "mangrove_biomass_loss_2018",
      windowSum("mangrove_carbon_emissions_2001") as "mangrove_carbon_emissions_2001",
      windowSum("mangrove_carbon_emissions_2002") as "mangrove_carbon_emissions_2002",
      windowSum("mangrove_carbon_emissions_2003") as "mangrove_carbon_emissions_2003",
      windowSum("mangrove_carbon_emissions_2004") as "mangrove_carbon_emissions_2004",
      windowSum("mangrove_carbon_emissions_2005") as "mangrove_carbon_emissions_2005",
      windowSum("mangrove_carbon_emissions_2006") as "mangrove_carbon_emissions_2006",
      windowSum("mangrove_carbon_emissions_2007") as "mangrove_carbon_emissions_2007",
      windowSum("mangrove_carbon_emissions_2008") as "mangrove_carbon_emissions_2008",
      windowSum("mangrove_carbon_emissions_2009") as "mangrove_carbon_emissions_2009",
      windowSum("mangrove_carbon_emissions_2010") as "mangrove_carbon_emissions_2010",
      windowSum("mangrove_carbon_emissions_2011") as "mangrove_carbon_emissions_2011",
      windowSum("mangrove_carbon_emissions_2012") as "mangrove_carbon_emissions_2012",
      windowSum("mangrove_carbon_emissions_2013") as "mangrove_carbon_emissions_2013",
      windowSum("mangrove_carbon_emissions_2014") as "mangrove_carbon_emissions_2014",
      windowSum("mangrove_carbon_emissions_2015") as "mangrove_carbon_emissions_2015",
      windowSum("mangrove_carbon_emissions_2016") as "mangrove_carbon_emissions_2016",
      windowSum("mangrove_carbon_emissions_2017") as "mangrove_carbon_emissions_2017",
      windowSum("mangrove_carbon_emissions_2018") as "mangrove_carbon_emissions_2018"
    )
  }
}
