package org.globalforestwatch.summarystats.annualupdate_minimal

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.summarystats.SummaryAnalysis
import org.globalforestwatch.util.Util.getAnyMapValue

object AnnualUpdateMinimalAnalysis extends SummaryAnalysis {
  val name = "annualupdate_minimal"

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val summaryRDD: RDD[(FeatureId, AnnualUpdateMinimalSummary)] =
      AnnualUpdateMinimalRDD(
        featureRDD,
        AnnualUpdateMinimalGrid.blockTileGrid,
        kwargs
      )

    val summaryDF =
      AnnualUpdateMinimalDFFactory(featureType, summaryRDD, spark).getDataFrame

    //    val maybeOutputPartitions:Option[Int] = getAnyMapValue(kwargs,"maybeOutputPartitions")
    //    val outputPartitionCount =
    //      maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

    summaryDF.repartition($"id", $"data_group")

    val runOutputUrl: String = getOutputUrl(kwargs)

    AnnualUpdateMinimalExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }
}
