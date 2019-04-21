package org.globalforestwatch.treecoverloss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object TreeLossDFHelpers {

  val spark: SparkSession = TreeLossSparkSession.spark
  import spark.implicits._

  val WindowPartitionOrder = Window
    .partitionBy($"m_feature_id", $"m_layers")
    .orderBy($"m_threshold".desc)

  def windowSum(e: Column): Column = sum(e).over(WindowPartitionOrder)
  def windowSum(columnName: String): Column = windowSum(col(columnName))
}
