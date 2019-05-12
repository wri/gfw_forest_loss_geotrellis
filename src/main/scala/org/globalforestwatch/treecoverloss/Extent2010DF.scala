package org.globalforestwatch.treecoverloss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.DataFrameHelpers._
import org.globalforestwatch.util.WindowFunctions.windowSum

object Extent2010DF {

  val spark: SparkSession = TreeLossSparkSession()

  import spark.implicits._

  val lookup2010: Map[String, String] = Map("threshold_2010" -> "threshold")

  def sumArea(df: DataFrame): DataFrame = {

    validatePresenceOfColumns(
      df,
      Seq("feature_id", "layers", "threshold_2010", "area")
    )

    df.select(
        df.columns
          .map(c => col(c).as(lookup2010.getOrElse(c, c))): _*
      )
      .groupBy("feature_id", "layers", "threshold")
      .agg(sum("area") as "area")
  }

  def joinMaster(masterDF: DataFrame)(df: DataFrame): DataFrame = {

    validatePresenceOfColumns(
      df,
      Seq("feature_id", "layers", "threshold", "area")
    )
    validatePresenceOfColumns(
      masterDF,
      Seq("m_feature_id", "m_layers", "m_threshold")
    )
    df.join(
        masterDF,
        $"m_feature_id" <=> $"feature_id" &&
          $"m_layers" <=> $"layers" &&
          $"m_threshold" <=> $"threshold",
        "right_outer"
      )
      .na
      .fill(0.0, Seq("area"))
  }

  def aggregateByThreshold(df: DataFrame): DataFrame = {

    validatePresenceOfColumns(
      df,
      Seq("feature_id", "layers", "threshold", "area")
    )
    df.select(
      $"m_feature_id",
      $"m_layers",
      $"m_threshold",
      windowSum("area") as "extent2010"
    )
  }

}
