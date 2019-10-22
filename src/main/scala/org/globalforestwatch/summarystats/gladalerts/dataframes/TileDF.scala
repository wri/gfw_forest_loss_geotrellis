package org.globalforestwatch.summarystats.gladalerts.dataframes

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

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
      $"data_group.alertDate" as "alert__date",
      $"data_group.isConfirmed" as "is__confirmed_alert",
      $"data.totalAlerts" as "alert__count"

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
        "alert__date",
        "is__confirmed_alert",
        "alert__count"
      )
    )

    df.groupBy($"x", $"y", $"z", $"alert__date", $"is__confirmed_alert")
      .agg(sum($"alert__count") as "alert__count")
  }
}
