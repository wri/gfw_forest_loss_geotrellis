package org.globalforestwatch.summarystats.gladalerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.summarystats.SummaryAnalysis
import org.globalforestwatch.util.Util._

object GladAlertsAnalysis extends SummaryAnalysis {
  val name = "gladalerts"

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val summaryRDD: RDD[(FeatureId, GladAlertsSummary)] =
      GladAlertsRDD(featureRDD, GladAlertsGrid.blockTileGrid, kwargs)

    val summaryDF =
      GladAlertsDFFactory(featureType, summaryRDD, spark).getDataFrame

//    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
//    val outputPartitionCount =
//      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition($"id", $"data_group")

    val runOutputUrl: String = getOutputUrl(kwargs)

    GladAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }
}
