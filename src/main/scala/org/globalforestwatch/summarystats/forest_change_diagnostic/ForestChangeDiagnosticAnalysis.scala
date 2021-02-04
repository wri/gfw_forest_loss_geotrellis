package org.globalforestwatch.summarystats.forest_change_diagnostic

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util.getAnyMapValue

object ForestChangeDiagnosticAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    val summaryRDD: RDD[(FeatureId, ForestChangeDiagnosticSummary)] =
      ForestChangeDiagnosticRDD(
        featureRDD,
        ForestChangeDiagnosticGrid.blockTileGrid,
        kwargs
      )

    val summaryDF =
      ForestChangeDiagnosticDFFactory(featureType, summaryRDD, spark).getDataFrame

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/forest_change_diagnostic_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    ForestChangeDiagnosticExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }
}
