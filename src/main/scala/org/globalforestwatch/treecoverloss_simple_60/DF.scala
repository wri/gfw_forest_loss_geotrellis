package org.globalforestwatch.treecoverloss_simple_60

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._

object DF {

  val spark: SparkSession = TreeLossSparkSession()

  import spark.implicits._

  def unpackValues(df: DataFrame): DataFrame = {

    validatePresenceOfColumns(
      df,
      Seq("feature_id", "threshold", "extent_2010", "total_area", "year_data")
    )

    df.select(
      $"feature_id.cell_id" as "cell_id",
      $"threshold",
      $"extent_2010",
      $"total_area",
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
      'year_data.getItem(17).getItem("area_loss") as "area_loss_2018"
    )
  }

}
