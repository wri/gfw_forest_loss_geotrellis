package org.globalforestwatch.summarystats.carbonflux_custom_area

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util.getAnyMapValue

object CarbonCustomAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val summaryRDD: RDD[(FeatureId, CarbonCustomSummary)] =
      CarbonCustomRDD(featureRDD, CarbonCustomGrid.blockTileGrid, kwargs)

    val summaryDF =
      CarbonCustomDFFactory(featureType, summaryRDD, spark).getDataFrame

    //    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
    //    val outputPartitionCount =
    //      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition($"id", $"dataGroup")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/carbonflux_custom_analysis_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    CarbonCustomExport.export(featureType, summaryDF, runOutputUrl, kwargs)
  }
}
