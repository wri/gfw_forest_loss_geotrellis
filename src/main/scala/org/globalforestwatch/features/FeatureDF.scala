package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.apache.spark.sql.functions.{split, struct}
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
         |SELECT ST_Point(CAST($lonField AS Decimal(24,10)),CAST($latField AS Decimal(24,10))) AS pointshape, *
         |FROM $viewName
      """.stripMargin)

    spatialDf
  }

  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            featureType: String,
            filters: Map[String, Any],
            spark: SparkSession,
            wkbField: String): DataFrame = {
    import spark.implicits._

    val featureDF: DataFrame = apply(input, featureObj, filters, spark)
    val featureDFWithGeom = featureDF
      .selectExpr(s"ST_PrecisionReduce(ST_GeomFromWKB(${wkbField}), 13) AS polyshape",  "*")
      .where("geom != '0106000020E610000000000000'")

    featureType match {
      case "gadm" =>
        featureDFWithGeom.select(
          $"polyshape",
          struct(
            $"gid_0" as "iso",
            split(split($"gid_1", "\\.")(1), "_")(0) as "adm1",
            split(split($"gid_2", "\\.")(2), "_")(0) as "adm2"
          ) as "featureId"
        )
      case "wdpa" =>
        featureDFWithGeom.select(
          $"polyshape",
          struct(
            $"wdpaid".cast("Int") as "wdpaId",  $"name" as "name", $"iucn_cat" as "iucnCat",  $"iso",  $"status"
          ) as "featureId"
        )
      case "feature" =>
        featureDFWithGeom.select($"polyshape", struct($"fid".cast("Int") as "featureId") as "featureId")
      case "geostore" =>
        featureDFWithGeom.select($"polyshape", struct($"geostore_id" as "geostoreId") as "featureId")
    }
  }
}
