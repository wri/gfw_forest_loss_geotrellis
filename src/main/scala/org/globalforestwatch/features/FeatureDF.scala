package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.apache.spark.sql.{DataFrame, SparkSession}


object FeatureDF {
  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            filters: Map[String, Any],
            spark: SparkSession,
            delimiter: String = "\t"): DataFrame =
    spark.read
      .options(Map("header" -> "true", "delimiter" -> delimiter, "escape" -> "\""))
      .csv(input.toList: _*)
      .transform(featureObj.filter(filters))

}
