package org.globalforestwatch.summarystats.annualupdate_minimal

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.summarystats.SummaryAnalysis

object AnnualUpdateMinimalAnalysis extends SummaryAnalysis {
  val name = "annualupdate_minimal"

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): DataFrame = {

    import spark.implicits._

    val summaryRDD: RDD[(FeatureId, AnnualUpdateMinimalSummary)] =
      AnnualUpdateMinimalRDD(
        featureRDD,
        AnnualUpdateMinimalGrid.blockTileGrid,
        kwargs
      )

    val summaryDF = AnnualUpdateMinimalDFFactory(featureType, summaryRDD, spark).getDataFrame
    summaryDF.repartition($"id", $"data_group")
  }
}
