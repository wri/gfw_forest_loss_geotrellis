package org.globalforestwatch.gladalerts

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util._

object GladAlertsSummaryAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            part: HashPartitioner,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._
    val summaryRDD: RDD[(FeatureId, GladAlertsSummary)] =
      GladAlertsRDD(featureRDD, GladAlertsGrid.blockTileGrid, part)

    val summaryDF =
      GladAlertsSummaryDFFactory(featureType, summaryRDD, spark).getDataFrame

//    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
//    val outputPartitionCount =
//      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition(partitionExprs = $"id")

    GladAlertsExport.export(
      featureType,
      summaryDF,
      getAnyMapValue[String](kwargs, "outputUrl")
    )
  }
}
