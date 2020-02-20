package org.globalforestwatch.summarystats

import cats.data.NonEmptyList
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureDF, FeatureId, FeatureRDD}
import org.globalforestwatch.summarystats.annualupdate.AnnualUpdateAnalysis
import org.globalforestwatch.summarystats.annualupdate_minimal.AnnualUpdateMinimalAnalysis
import org.globalforestwatch.summarystats.carbonflux.CarbonFluxAnalysis
import org.globalforestwatch.summarystats.carbon_sensitivity.CarbonSensitivityAnalysis
import org.globalforestwatch.summarystats.firealerts.FireAlertsAnalysis
import org.globalforestwatch.summarystats.gladalerts.GladAlertsAnalysis
import org.globalforestwatch.summarystats.treecoverloss.TreeLossAnalysis

case class SummaryAnalysisFactory(analysis: String,
                                  featureObj: org.globalforestwatch.features.Feature,
                                  featureType: String,
                                  featureUris: NonEmptyList[String],
                                  spark: SparkSession,
                                  kwargs: Map[String, Any]) {

  val runAnalysis: Unit =
    if (analysis != "firealerts") {
      // ref: https://github.com/databricks/spark-csv
      val featuresDF: DataFrame =
        FeatureDF(featureUris, featureObj, kwargs, spark)

      /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
      val featureRDD: RDD[Feature[Geometry, FeatureId]] =
        FeatureRDD(featuresDF, featureObj)

      val inputPartitionMultiplier = 64

      val part = new HashPartitioner(
        partitions = featureRDD.getNumPartitions * inputPartitionMultiplier
      )

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
    } else {
      val runAnalysis: Unit =
        FireAlertsAnalysis(
          featureObj,
          featureType: String,
          spark: SparkSession,
          kwargs: Map[String, Any]
        )
    }
}
