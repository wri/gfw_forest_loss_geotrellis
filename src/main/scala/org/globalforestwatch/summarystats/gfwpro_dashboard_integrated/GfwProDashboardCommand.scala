package org.globalforestwatch.summarystats.gfwpro_dashboard_integrated

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import geotrellis.vector.Geometry
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features._
import org.locationtech.jts.geom.Geometry

object GfwProDashboardCommand extends SummaryCommand {

  val contextualFeatureUrlOpt: Opts[NonEmptyList[String]] = Opts
    .options[String](
      "contextual_feature_url",
      help = "URI of contextual features in TSV format"
    )

  val contextualFeatureTypeOpt: Opts[String] = Opts
    .option[String](
      "contextual_feature_type",
      help = "Type of contextual features"
    )

  val gfwProDashboardIntegratedCommand: Opts[Unit] = Opts.subcommand(
    name = GfwProDashboardAnalysis.name,
    help = "Compute summary statistics for GFW Pro Dashboard."
  ) {
    (
      defaultOptions,
      optionalFireAlertOptions,
      featureFilterOptions,
      contextualFeatureUrlOpt,
      contextualFeatureTypeOpt
      ).mapN { (default, fireAlert, filterOptions, contextualFeatureUrl, contextualFeatureType) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput,
        "config" -> GfwConfig.get
      )
      // TODO: move building the feature object into options
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { implicit spark =>
        val featureRDD = ValidatedFeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures)

        val fireAlertRDD = fireAlert.alertSource match {
          case Some(alertSource) =>
            FireAlertRDD(spark, fireAlert.alertType, alertSource, FeatureFilter.empty)
          case None =>
            // If no sources provided, just create an empty RDD
            val spatialRDD = new SpatialRDD[Geometry]
            spatialRDD.rawSpatialRDD = spark.sparkContext.emptyRDD[Geometry].toJavaRDD()
            spatialRDD
        }

        GfwProDashboardAnalysis(
          featureRDD,
          default.featureType,
          contextualFeatureType = contextualFeatureType,
          contextualFeatureUrl = contextualFeatureUrl,
          fireAlertRDD,
          spark,
          kwargs
        )
      }
    }
  }
}
