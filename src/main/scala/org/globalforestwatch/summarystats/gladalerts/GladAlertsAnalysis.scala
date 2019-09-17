package org.globalforestwatch.summarystats.gladalerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util._

object GladAlertsAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            part: HashPartitioner,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._
    val summaryRDD: RDD[(FeatureId, GladAlertsSummary)] =
      GladAlertsRDD(featureRDD, GladAlertsGrid.blockTileGrid, part, kwargs)

    val summaryDF =
      GladAlertsDFFactory(featureType, summaryRDD, spark).getDataFrame

//    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
//    val outputPartitionCount =
//      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition(partitionExprs = $"id")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/gladAlerts_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    GladAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }
}
