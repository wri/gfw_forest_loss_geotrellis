package org.globalforestwatch.summarystats.integrated_alerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.summarystats.SummaryAnalysis
import org.globalforestwatch.util.Util._

object IntegratedAlertsAnalysis extends SummaryAnalysis {
  val name = "integrated_alerts"

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val summaryRDD: RDD[(FeatureId, IntegratedAlertsSummary)] =
      IntegratedAlertsRDD(featureRDD, IntegratedAlertsGrid.blockTileGrid, kwargs)

    val summaryDF =
      IntegratedAlertsDFFactory(featureType, summaryRDD, spark).getDataFrame

//    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
//    val outputPartitionCount =
//      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition($"id", $"data_group")

    val runOutputUrl: String = getOutputUrl(kwargs)

    IntegratedAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }
}
