package org.globalforestwatch.summarystats.carbonflux_minimal

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util.getAnyMapValue

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object CarbonFluxMinimalAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {
    import spark.implicits._

    val summaryRDD: RDD[(FeatureId, CarbonFluxMinimalSummary)] =
      CarbonFluxMinimalRDD(featureRDD, CarbonFluxMinimalGrid.blockTileGrid, kwargs)

    val summaryDF =
      CarbonFluxMinimalDFFactory(featureType, summaryRDD, spark).getDataFrame

    //    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
    //    val outputPartitionCount =
    //      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition(partitionExprs = $"id")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/carbonflux_minimal_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    CarbonFluxMinimalExport.export(featureType, summaryDF, runOutputUrl, kwargs)
  }
}
