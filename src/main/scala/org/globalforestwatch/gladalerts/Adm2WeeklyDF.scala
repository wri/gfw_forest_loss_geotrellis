package org.globalforestwatch.gladalerts

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._
import org.globalforestwatch.treecoverloss.TreeLossSparkSession

object Adm2WeeklyDF {

  val spark: SparkSession = GladAlertsSparkSession()
  import spark.implicits._

  def sumAlerts(df: DataFrame): DataFrame = {
    df.select(
        $"iso",
        $"adm1",
        $"adm2",
        year($"alert_date") as "year",
        weekofyear($"alert_date") as "week",
        $"is_confirmed",
        $"alerts",
        $"area",
        $"co2_emissions"
      )
      .groupBy($"iso", $"adm1", $"adm2", $"year", $"week", $"is_confirmed")
      .agg(
        sum("alerts") as "alerts",
        sum("area") as "area",
        sum("co2_emissions") as "co2_emissions"
      )
  }
}
