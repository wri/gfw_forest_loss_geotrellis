package org.globalforestwatch.gladalerts

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Adm2DailyDF {

  val spark: SparkSession = GladAlertsSparkSession()
  import spark.implicits._

  def sumAlerts(df: DataFrame): DataFrame = {
    df.filter($"z" === 0)
      .groupBy($"iso", $"adm1", $"adm2", $"alert_date", $"is_confirmed")
      .agg(
        sum("alerts") as "alerts",
        sum("area") as "area",
        sum("co2") as "co2_emissions"
      )
  }
}
