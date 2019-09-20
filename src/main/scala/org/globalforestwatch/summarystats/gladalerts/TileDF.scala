package org.globalforestwatch.summarystats.gladalerts

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object TileDF {

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "id",
        "data_group",
        "data"
      )
    )

    df.select(
      $"data_group.tile.x" as "x",
      $"data_group.tile.y" as "y",
      $"data_group.tile.z" as "z",
      $"data_group.alertDate" as "alert_date",
      $"data_group.isConfirmed" as "alert_confirmation_status",
      $"data.totalAlerts" as "alert_count"

    )
  }

  def sumAlerts(df: DataFrame): DataFrame = {

    val spark = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "x",
        "y",
        "z",
        "alert_date",
        "alert_confirmation_status",
        "alert_count"
      )
    )

    df.groupBy($"x", $"y", $"z", $"alert_date", $"alert_confirmation_status")
      .agg(sum($"alert_count") as "alert_count")
  }
}
