package org.globalforestwatch.treecoverloss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object TreeLossDFHelpers {

  //  val spark: SparkSession = TreeLossSparkSession.spark
  //  import spark.implicits._

  val WindowPartitionOrder: WindowSpec = Window
    .partitionBy(col("m_feature_id"), col("m_layers"))
    .orderBy(col("m_threshold").desc)

  def windowSum(e: Column): Column = sum(e).over(WindowPartitionOrder)
  def windowSum(columnName: String): Column = windowSum(col(columnName))
}
