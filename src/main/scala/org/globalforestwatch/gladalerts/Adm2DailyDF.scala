package org.globalforestwatch.gladalerts

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.treecoverloss.TreeLossSparkSession

object Adm2DailyDF {

  val spark: SparkSession = TreeLossSparkSession.spark
  import spark.implicits._

  def sumAlerts(df: DataFrame): DataFrame = {
    df.groupBy($"iso", $"adm1", $"adm2", $"alert_date", $"is_confirmed")
      .agg(
        sum("alerts") as "alerts",
        sum("area") as "area",
        sum("co2") as "co2_emissions"
      )
  }
}
