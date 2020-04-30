package org.globalforestwatch.summarystats.firealerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{FeatureDF, FeatureFactory, FeatureId}
import org.globalforestwatch.summarystats.firealerts.FireAlertsRDD.SUMMARY
import org.globalforestwatch.util.Util._
import org.datasyslab.geospark.spatialRDD.{PointRDD, SpatialRDD}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import java.util.HashSet

import cats.data.NonEmptyList

import collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.spark.api.java.JavaRDD
import org.datasyslab.geosparksql.utils.Adapter

object FireAlertsAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            part: HashPartitioner,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val layoutDefinition = fireAlertType match {
      case "viirs" => ViirsGrid.blockTileGrid
      case "modis" => ModisGrid.blockTileGrid
    }

    val summaryRDD: RDD[(FeatureId, FireAlertsSummary)] =
      FireAlertsRDD(featureRDD, layoutDefinition, part, kwargs)

    val summaryDF =
      FireAlertsDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame

    summaryDF.repartition(partitionExprs = $"featureId")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      s"/${DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now)}" +
      s"/firealerts/$fireAlertType"

    FireAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }
}
