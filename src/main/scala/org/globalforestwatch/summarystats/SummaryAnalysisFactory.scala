package org.globalforestwatch.summarystats

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.summarystats.annualupdate.AnnualUpdateAnalysis
import org.globalforestwatch.summarystats.gladalerts.GladAlertsSummaryAnalysis
import org.globalforestwatch.summarystats.treecoverloss.TreeLossAnalysis

case class SummaryAnalysisFactory(analysis: String,
                                  featureRDD: RDD[Feature[Geometry, FeatureId]],
                                  featureType: String,
                                  part: HashPartitioner,
                                  spark: SparkSession,
                                  kwargs: Map[String, Any]) {

  val runAnalysis =
    analysis match {
      case "annualupdate" =>
        AnnualUpdateAnalysis(
          featureRDD: RDD[Feature[Geometry, FeatureId]],
          featureType: String,
          part: HashPartitioner,
          spark: SparkSession,
          kwargs: Map[String, Any]
        )
      case "annualupdate_minimal" => ???
      case "carbonflux"           => ???
      case "gladalerts" =>
        GladAlertsSummaryAnalysis(
          featureRDD: RDD[Feature[Geometry, FeatureId]],
          featureType: String,
          part: HashPartitioner,
          spark: SparkSession,
          kwargs: Map[String, Any]
        )
      case "treecoverloss" =>
        TreeLossAnalysis(
          featureRDD: RDD[Feature[Geometry, FeatureId]],
          featureType: String,
          part: HashPartitioner,
          spark: SparkSession,
          kwargs: Map[String, Any]
        )
    }

}
