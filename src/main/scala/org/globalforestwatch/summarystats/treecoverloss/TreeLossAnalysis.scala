package org.globalforestwatch.summarystats.treecoverloss

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util.getAnyMapValue

object TreeLossAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {
    import spark.implicits._

    val summaryRDD: RDD[(FeatureId, TreeLossSummary)] =
      TreeLossRDD(featureRDD, TreeLossGrid.blockTileGrid, kwargs)

    val summaryDF =
      TreeLossDFFactory(featureType, summaryRDD, spark).getDataFrame

    //    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
    //    val outputPartitionCount =
    //      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition(partitionExprs = $"id")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/treecoverloss_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    TreeLossExport.export(featureType, summaryDF, runOutputUrl, kwargs)
  }
}
