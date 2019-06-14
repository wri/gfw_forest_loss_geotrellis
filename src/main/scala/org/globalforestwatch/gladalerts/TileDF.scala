package org.globalforestwatch.gladalerts

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}
import org.globalforestwatch.annualupdate.TreeLossSparkSession

object TileDF {

  val spark: SparkSession = TreeLossSparkSession()
  import spark.implicits._

  def sumAlerts(df: DataFrame): DataFrame = {
    df.groupBy($"x", $"y", $"z", $"alert_date", $"is_confirmed")
      .agg(sum($"alert_count") as "alert_count")
  }
}
