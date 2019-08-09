package org.globalforestwatch.treecoverloss

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions.sum

object DF {



  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "feature_id",
        "threshold",
        "tcd_year",
        "primary_forest",
        "extent_2000",
        "extent_2010",
        "total_area",
        "total_gain",
        "total_biomass",
        "avg_biomass_per_ha",
        "total_co2",
        "year_data"
      )
    )

    df.select(
      $"feature_id",
      $"threshold",
      $"tcd_year",
      $"primary_forest",
      $"extent_2000",
      $"extent_2010",
      $"total_area",
      $"total_gain",
      $"total_biomass",
      $"avg_biomass_per_ha",
      $"total_co2",
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
        .getItem("carbon_emissions") as "co2_emissions_2001",
      'year_data
        .getItem(1)
        .getItem("carbon_emissions") as "co2_emissions_2002",
      'year_data
        .getItem(2)
        .getItem("carbon_emissions") as "co2_emissions_2003",
      'year_data
        .getItem(3)
        .getItem("carbon_emissions") as "co2_emissions_2004",
      'year_data
        .getItem(4)
        .getItem("carbon_emissions") as "co2_emissions_2005",
      'year_data
        .getItem(5)
        .getItem("carbon_emissions") as "co2_emissions_2006",
      'year_data
        .getItem(6)
        .getItem("carbon_emissions") as "co2_emissions_2007",
      'year_data
        .getItem(7)
        .getItem("carbon_emissions") as "co2_emissions_2008",
      'year_data
        .getItem(8)
        .getItem("carbon_emissions") as "co2_emissions_2009",
      'year_data
        .getItem(9)
        .getItem("carbon_emissions") as "co2_emissions_2010",
      'year_data
        .getItem(10)
        .getItem("carbon_emissions") as "co2_emissions_2011",
      'year_data
        .getItem(11)
        .getItem("carbon_emissions") as "co2_emissions_2012",
      'year_data
        .getItem(12)
        .getItem("carbon_emissions") as "co2_emissions_2013",
      'year_data
        .getItem(13)
        .getItem("carbon_emissions") as "co2_emissions_2014",
      'year_data
        .getItem(14)
        .getItem("carbon_emissions") as "co2_emissions_2015",
      'year_data
        .getItem(15)
        .getItem("carbon_emissions") as "co2_emissions_2016",
      'year_data
        .getItem(16)
        .getItem("carbon_emissions") as "co2_emissions_2017",
      'year_data
        .getItem(17)
        .getItem("carbon_emissions") as "co2_emissions_2018"
    )
  }

  def primaryForestFilter(include: Boolean)(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    if (include) df
    else {
      df.groupBy($"feature_id", $"threshold", $"tcd_year")
        .agg(
          sum("total_area") as "total_area",
          sum("extent_2000") as "extent_2000",
          sum("extent_2010") as "extent_2010",
          sum("total_gain") as "total_gain",
          sum("total_biomass") as "total_biomass",
          sum($"avg_biomass_per_ha" * $"extent_2000") / sum($"extent_2000") as "avg_biomass_per_ha",
          sum("total_co2") as "total_co2",
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
          sum("co2_emissions_2001") as "co2_emissions_2001",
          sum("co2_emissions_2002") as "co2_emissions_2002",
          sum("co2_emissions_2003") as "co2_emissions_2003",
          sum("co2_emissions_2004") as "co2_emissions_2004",
          sum("co2_emissions_2005") as "co2_emissions_2005",
          sum("co2_emissions_2006") as "co2_emissions_2006",
          sum("co2_emissions_2007") as "co2_emissions_2007",
          sum("co2_emissions_2008") as "co2_emissions_2008",
          sum("co2_emissions_2009") as "co2_emissions_2009",
          sum("co2_emissions_2010") as "co2_emissions_2010",
          sum("co2_emissions_2011") as "co2_emissions_2011",
          sum("co2_emissions_2012") as "co2_emissions_2012",
          sum("co2_emissions_2013") as "co2_emissions_2013",
          sum("co2_emissions_2014") as "co2_emissions_2014",
          sum("co2_emissions_2015") as "co2_emissions_2015",
          sum("co2_emissions_2016") as "co2_emissions_2016",
          sum("co2_emissions_2017") as "co2_emissions_2017",
          sum("co2_emissions_2018") as "co2_emissions_2018"
        )
    }
  }

}
