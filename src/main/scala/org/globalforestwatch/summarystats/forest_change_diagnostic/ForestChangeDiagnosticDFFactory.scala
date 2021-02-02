package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, SimpleFeatureId}

case class ForestChangeDiagnosticDFFactory(
  featureType: String,
  summaryRDD: RDD[(FeatureId, ForestChangeDiagnosticSummary)],
  spark: SparkSession
) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "feature" => getFeatureDataFrame
      case _ =>
        throw new IllegalArgumentException("Not a valid FeatureId")
    }
  }

  private def getFeatureDataFrame: DataFrame = {
    summaryRDD
      .map {
        case (id, summary) =>
          id match {
            case simpleId: SimpleFeatureId =>
              ForestChangeDiagnosticRowSimple(simpleId, summary.stats)
            case _ =>
              throw new IllegalArgumentException("Not a SimpleFeatureId")
          }
      }
      .toDF("id", "data")
  }

}
