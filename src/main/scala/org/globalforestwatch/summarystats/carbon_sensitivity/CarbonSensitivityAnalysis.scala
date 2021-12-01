package org.globalforestwatch.summarystats.carbon_sensitivity

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.summarystats.SummaryAnalysis
import org.globalforestwatch.util.Util.getAnyMapValue

object CarbonSensitivityAnalysis extends SummaryAnalysis {
  val name = "carbon_sensitivity"

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val model: String = getAnyMapValue[String](kwargs, "sensitivityType")

    val summaryRDD: RDD[(FeatureId, CarbonSensitivitySummary)] =
      CarbonSensitivityRDD(featureRDD, CarbonSensitivityGrid.blockTileGrid, kwargs)

    val summaryDF =
      CarbonSensitivityDFFactory(featureType, summaryRDD, spark).getDataFrame

    //    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
    //    val outputPartitionCount =
    //      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition($"id", $"dataGroup")

    val runOutputUrl: String = getOutputUrl(kwargs)

    CarbonSensitivityExport.export(featureType, summaryDF, runOutputUrl, kwargs)
  }
}
