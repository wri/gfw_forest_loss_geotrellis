package org.globalforestwatch.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, sum}

object WindowFunctions {

  // TODO: column names should not be hard coded here
  //  best to include partition and order columns to the windowSum parameter list and initialize the window here
  val WindowPartitionOrder: WindowSpec = Window
    .partitionBy(col("m_feature_id"), col("m_layers"))
    .orderBy(col("m_threshold").desc)

  def windowSum(e: Column): Column = sum(e).over(WindowPartitionOrder)
  def windowSum(columnName: String): Column = windowSum(col(columnName))
}
