package org.globalforestwatch.gladalerts

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}
import org.globalforestwatch.treecoverloss.TreeLossSparkSession

object IsoWeeklyDF {

  val spark: SparkSession = TreeLossSparkSession()
  import spark.implicits._

  def sumAlerts(df: DataFrame): DataFrame = {
    df.groupBy($"iso", $"year", $"week", $"is_confirmed")
      .agg(
        sum("alerts") as "alerts",
        sum("area") as "area",
        sum("co2_emissions") as "co2_emissions"
      )
  }
}
