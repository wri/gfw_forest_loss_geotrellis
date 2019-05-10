package org.globalforestwatch.gladalerts

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}
import org.globalforestwatch.treecoverloss.TreeLossSparkSession

object Adm1WeeklyDF {

  val spark: SparkSession = TreeLossSparkSession.spark
  import spark.implicits._

  def sumAlerts(df: DataFrame): DataFrame = {
    df.groupBy($"iso", $"adm1", $"year", $"week", $"is_confirmed")
      .agg(
        sum("alerts") as "alerts",
        sum("area") as "area",
        sum("co2_emissions") as "co2_emissions"
      )
  }
}
