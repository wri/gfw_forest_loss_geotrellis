package org.globalforestwatch.summarystats.gladalerts

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object TileDF {


  def sumAlerts(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    df.groupBy($"x", $"y", $"z", $"alert_date", $"is_confirmed")
      .agg(sum($"alert_count") as "alert_count")
  }
}
