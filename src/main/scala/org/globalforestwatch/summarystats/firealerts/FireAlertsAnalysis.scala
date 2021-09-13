package org.globalforestwatch.summarystats.firealerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import geotrellis.vector.Feature
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features._
import org.globalforestwatch.util.Util._
import cats.data.NonEmptyList
import geotrellis.vector
import org.globalforestwatch.summarystats.SummaryAnalysis


object FireAlertsAnalysis extends SummaryAnalysis {

  val name = "firealerts"

  def apply(featureRDD: RDD[Feature[vector.Geometry, FeatureId]],
            featureType: String,
            featureFilter: FeatureFilter,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val layoutDefinition = fireAlertType match {
      case "viirs" | "burned_areas" => ViirsGrid.blockTileGrid
      case "modis" => ModisGrid.blockTileGrid
    }

    val partition = fireAlertType match {
      case "modis" | "viirs" | "burned_areas" => false
      case _ => true
    }

    val summaryRDD: RDD[(FeatureId, FireAlertsSummary)] =
      FireAlertsRDD(featureRDD, layoutDefinition, kwargs, partition = partition)

    val summaryDF = fireAlertType match {
      case "modis" | "viirs" =>
        joinWithFeatures(summaryRDD, featureType, featureFilter, spark, kwargs)
      case "burned_areas" =>
        FireAlertsDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame
    }

    summaryDF.repartition(partitionExprs = $"featureId")

    val runOutputUrl: String = getOutputUrl(kwargs, s"${name}_${fireAlertType}")


    FireAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }

  def joinWithFeatures(summaryRDD: RDD[(FeatureId, FireAlertsSummary)],
                       featureType: String,
                       featureFilter: FeatureFilter,
                       spark: SparkSession,
                       kwargs: Map[String, Any]): DataFrame = {
    val fireDF = FireAlertsDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame

    val firePointDF = fireDF
      .selectExpr("ST_Point(CAST(fireId.lon AS Decimal(24,10)),CAST(fireId.lat AS Decimal(24,10))) AS pointshape", "*")

    val featureUris: NonEmptyList[String] = getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris")

    val featureDF = SpatialFeatureDF(featureUris, featureType, featureFilter, "geom", spark)

    firePointDF
      .join(featureDF)
      .where("ST_Intersects(pointshape, polyshape)")
  }
}
