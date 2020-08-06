package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.apache.spark.sql.functions.{split, struct}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


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
         |SELECT ST_Point(CAST($lonField AS Decimal(24,10)),CAST($latField AS Decimal(24,10))) AS pointshape, *
         |FROM $viewName
      """.stripMargin)

    spatialDf
  }

  /*
   * Use GeoSpark to directly generate a DataFrame with a geometry column
   */
  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            featureType: String,
            filters: Map[String, Any],
            spark: SparkSession,
            wkbField: String): DataFrame = {

    val featureDF: DataFrame = apply(input, featureObj, filters, spark)
    val emptyPolygonWKB = "0106000020E610000000000000"

    featureDF
      .selectExpr(
        s"ST_PrecisionReduce(ST_GeomFromWKB(${wkbField}), 13) AS polyshape",
        s"struct(${featureObj.featureIdExpr}) as featureId")
      .where(s"${wkbField} != '${emptyPolygonWKB}'")
  }
}
