package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.apache.spark.sql.{DataFrame, SparkSession}


object FeatureDF {
  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            filters: Map[String, Any],
            spark: SparkSession): DataFrame =
    spark.read
      .options(Map("header" -> "true", "delimiter" -> "\t", "escape" -> "\""))
      .csv(input.toList: _*)
      .transform(featureObj.filter(filters))


  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            filters: Map[String, Any],
            spark: SparkSession,
            lonField: String,
            latField: String): DataFrame = {
    val df: DataFrame = apply(input, featureObj, filters, spark)

    val viewName = featureObj.getClass.getSimpleName.dropRight(1).toLowerCase
    df.createOrReplaceTempView(viewName)
    val spatialDf = spark.sql(
      s"""
         |SELECT ST_Point(CAST($lonField AS Decimal(24,10)),CAST($latField AS Decimal(24,10))) AS geometry, *
         |FROM $viewName
      """.stripMargin)

    spatialDf
  }

  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            filters: Map[String, Any],
            spark: SparkSession,
            wkbField: String): DataFrame = {
    val df: DataFrame = apply(input, featureObj, filters, spark)

    val viewName = featureObj.getClass.getSimpleName.dropRight(1).toLowerCase
    df.createOrReplaceTempView(viewName)

    // Create Geometry field, reducing precision of coordinates and filtering out
    // any empty polygonss
    val spatialDf = spark.sql(
      s"""
         |SELECT ST_PrecisionReduce(ST_GeomFromWKB($wkbField), 13) AS geometry, *
         |FROM $viewName
         |WHERE geom != '0106000020E610000000000000'
      """.stripMargin)

    spatialDf
  }
}
