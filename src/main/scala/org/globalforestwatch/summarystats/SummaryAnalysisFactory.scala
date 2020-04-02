package org.globalforestwatch.summarystats

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.summarystats.annualupdate.AnnualUpdateAnalysis
import org.globalforestwatch.summarystats.annualupdate_minimal.AnnualUpdateMinimalAnalysis
import org.globalforestwatch.summarystats.carbonflux.CarbonFluxAnalysis
import org.globalforestwatch.summarystats.carbonflux_custom_area.CarbonCustomAnalysis
import org.globalforestwatch.summarystats.carbon_sensitivity.CarbonSensitivityAnalysis
import org.globalforestwatch.summarystats.gladalerts.GladAlertsAnalysis
import org.globalforestwatch.summarystats.treecoverloss.TreeLossAnalysis

case class SummaryAnalysisFactory(analysis: String,
                                  featureRDD: RDD[Feature[Geometry, FeatureId]],
                                  featureType: String,
                                  part: HashPartitioner,
                                  spark: SparkSession,
                                  kwargs: Map[String, Any]) {

  val runAnalysis: Unit =
    analysis match {
      case "annualupdate" =>
        AnnualUpdateAnalysis(
          featureRDD: RDD[Feature[Geometry, FeatureId]],
          featureType: String,
          part: HashPartitioner,
          spark: SparkSession,
          kwargs: Map[String, Any]
        )
      case "annualupdate_minimal" =>
        AnnualUpdateMinimalAnalysis(
          featureRDD: RDD[Feature[Geometry, FeatureId]],
          featureType: String,
          part: HashPartitioner,
          spark: SparkSession,
          kwargs: Map[String, Any]
        )
      case "carbonflux" =>
        CarbonFluxAnalysis(
          featureRDD: RDD[Feature[Geometry, FeatureId]],
          featureType: String,
          part: HashPartitioner,
          spark: SparkSession,
          kwargs: Map[String, Any]
        )
      case "carbon_custom" =>
        CarbonCustomAnalysis(
          featureRDD: RDD[Feature[Geometry, FeatureId]],
          featureType: String,
          part: HashPartitioner,
          spark: SparkSession,
          kwargs: Map[String, Any]
        )
     case "carbon_sensitivity" =>
        CarbonSensitivityAnalysis(
          featureRDD: RDD[Feature[Geometry, FeatureId]],
          featureType: String,
          part: HashPartitioner,
          spark: SparkSession,
          kwargs: Map[String, Any]
        )
      case "gladalerts" =>
        GladAlertsAnalysis(
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
      case _ =>
        throw new IllegalArgumentException("Not a valid analysis")
    }

}
