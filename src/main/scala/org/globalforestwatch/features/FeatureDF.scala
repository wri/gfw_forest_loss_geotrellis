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
         |SELECT ST_Point(CAST($lonField AS Decimal(24,20)),CAST($latField AS Decimal(24,20))) AS geometry, *
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
    val spatialDf = spark.sql(
      s"""
         |SELECT ST_GeomFromWKB($wkbField) AS geometry, *
         |FROM $viewName
      """.stripMargin)

    //val spatialRDD = new SpatialRDD[Geometry]
    //spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)

    spatialDf
  }
}
