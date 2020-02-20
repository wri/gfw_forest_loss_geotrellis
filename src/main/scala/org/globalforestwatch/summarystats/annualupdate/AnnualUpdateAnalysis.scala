package org.globalforestwatch.summarystats.annualupdate

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util.{getAnyMapValue, getKeyedFeatureRDD}

object AnnualUpdateAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            part: HashPartitioner,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val summaryRDD: RDD[(FeatureId, AnnualUpdateSummary)] =
      AnnualUpdateRDD(featureRDD, AnnualUpdateGrid.blockTileGrid, part, kwargs)

    val summaryDF =
      AnnualUpdateDFFactory(featureType, summaryRDD, spark).getDataFrame

    //    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
    //    val outputPartitionCount =
    //      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition($"id", $"data_group")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/annualupdate_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    AnnualUpdateExport.export(featureType, summaryDF, runOutputUrl, kwargs)
  }
}