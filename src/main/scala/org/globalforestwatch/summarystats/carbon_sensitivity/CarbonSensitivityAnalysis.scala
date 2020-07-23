package org.globalforestwatch.summarystats.carbon_sensitivity

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.layer.SpatialKey
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util.{getAnyMapValue, getKeyedFeatureRDD}

object CarbonSensitivityAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            part: HashPartitioner,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val model:String = getAnyMapValue[String](kwargs,"sensitivityType")

    val summaryRDD: RDD[(FeatureId, CarbonSensitivitySummary)] =
      CarbonSensitivityRDD(featureRDD, CarbonSensitivityGrid.blockTileGrid, Some(part), kwargs)

    val summaryDF =
      CarbonSensitivityDFFactory(featureType, summaryRDD, spark).getDataFrame

    //    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
    //    val outputPartitionCount =
    //      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition($"id", $"dataGroup")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      s"/carbon_sensitivity_${model}_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    CarbonSensitivityExport.export(featureType, summaryDF, runOutputUrl, kwargs)
  }
}
