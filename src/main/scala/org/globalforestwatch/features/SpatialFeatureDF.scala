package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.apache.spark.sql.{DataFrame, SparkSession}

object SpatialFeatureDF {
  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            filters: FeatureFilter,
            spark: SparkSession,
            lonField: String,
            latField: String): DataFrame = {
    val df: DataFrame = FeatureDF(input, featureObj, filters, spark)

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
            featureType: String,
            filters: FeatureFilter,
            wkbField: String,
            spark: SparkSession,
            delimiter: String = "\t"): DataFrame = {

    val featureObj: Feature = Feature(featureType)
    SpatialFeatureDF(input, featureObj, filters, wkbField, spark, delimiter)
  }

  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            filters: FeatureFilter,
            wkbField: String,
            spark: SparkSession,
            delimiter: String): DataFrame = {

    val featureDF: DataFrame =
      FeatureDF(input, featureObj, filters, spark, delimiter)
    val emptyPolygonWKB = "0106000020E610000000000000"

    featureDF
      .selectExpr(
        s"ST_PrecisionReduce(ST_GeomFromWKB(${wkbField}), 11) AS polyshape",
        s"struct(${featureObj.featureIdExpr}) as featureId"
      )
      .where(s"${wkbField} != '${emptyPolygonWKB}'")
  }
}
