package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.apache.spark.sql.{DataFrame, SparkSession}

object FeatureDF {

  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            filters: Map[String, Any],
            spark: SparkSession): DataFrame =
    spark.read
      .options(Map("header" -> "true", "delimiter" -> "\t"))
      .csv(input.toList: _*)
      .transform(featureObj.filter(filters))
}
