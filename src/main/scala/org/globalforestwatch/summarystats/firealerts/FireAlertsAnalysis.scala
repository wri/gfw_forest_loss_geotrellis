package org.globalforestwatch.summarystats.firealerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import geotrellis.vector.Feature
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureDF, FeatureFactory, FeatureId, SpatialFeatureDF}
import org.globalforestwatch.util.Util._
import cats.data.NonEmptyList
import geotrellis.vector


object FireAlertsAnalysis {
  def apply(featureRDD: RDD[Feature[vector.Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val layoutDefinition = fireAlertType match {
      case "viirs" => ViirsGrid.blockTileGrid
      case "modis" | "burned_areas" => ModisGrid.blockTileGrid
    }

    val summaryRDD: RDD[(FeatureId, FireAlertsSummary)] =
      FireAlertsRDD(featureRDD, layoutDefinition, kwargs, partition = false)

    val summaryDF = fireAlertType match {
      case "modis" | "viirs" =>
        joinWithFeatures(summaryRDD, featureType, spark, kwargs)
      case "burned_areas" =>
        BurnedAreasDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame
    }

    summaryDF.repartition(partitionExprs = $"featureId")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      s"/firealerts_${fireAlertType}_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    FireAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }

  def joinWithFeatures(summaryRDD: RDD[(FeatureId, FireAlertsSummary)],
                       featureType: String,
                       spark: SparkSession,
                       kwargs: Map[String, Any]): DataFrame = {
    val fireDF = BurnedAreasDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame

    val firePointDF = fireDF
      .selectExpr("ST_Point(CAST(fireId.lon AS Decimal(24,10)),CAST(fireId.lat AS Decimal(24,10))) AS pointshape", "*")

    val featureObj = FeatureFactory(featureType).featureObj
    val featureUris: NonEmptyList[String] = getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris")

    val featureDF = SpatialFeatureDF(featureUris, featureObj, kwargs, "geom", spark)

    firePointDF
      .join(featureDF)
      .where("ST_Intersects(pointshape, polyshape)")
  }
}
