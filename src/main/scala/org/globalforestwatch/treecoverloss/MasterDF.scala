package org.globalforestwatch.treecoverloss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.DataFrameHelpers._

object MasterDF {

  val spark: SparkSession = TreeLossSparkSession()

  import spark.implicits._

  def expandByThreshold(df: DataFrame): DataFrame = {

    validatePresenceOfColumns(df, Seq("feature_id", "layers", "area"))

    val thresholdDF: DataFrame =
      Seq(0, 10, 15, 20, 25, 30, 50, 75).toDF("threshold")

    df.groupBy("feature_id", "layers")
      .agg(sum("area") as "totalarea")
      .crossJoin(thresholdDF)
      .select(
        col("feature_id") as "m_feature_id",
        col("layers") as "m_layers",
        col("threshold") as "m_threshold",
        col("totalarea")
      )
  }
}
