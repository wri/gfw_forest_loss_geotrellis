package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._
import com.typesafe.scalalogging.LazyLogging
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.util.Config

object ForestChangeDiagnosticCommand extends SummaryCommand with LazyLogging {
  // Current range of years for UMD tree cover loss and country-specific tree cover loss.
  // Update TreeCoverLossYearEnd when new data becomes available.
  val TreeCoverLossYearStart: Int = 2001
  val TreeCoverLossYearEnd: Int = 2023

  val forestChangeDiagnosticCommand: Opts[Unit] = Opts.subcommand(
    name = ForestChangeDiagnosticAnalysis.name,
    help = "Compute summary statistics for GFW Pro Forest Change Diagnostic."
  ) {
    (
      defaultOptions,
      requiredFireAlertOptions,
      featureFilterOptions
    ).mapN { (default, fireAlert, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput,
        // Pin the version of gfw_integrated_alerts, so we don't make a data API request for 'latest'
        "config" -> GfwConfig.get(Some(NonEmptyList.one(Config("gfw_integrated_alerts", "v20231121"))))
      )

      if (!default.splitFeatures) logger.warn("Forcing splitFeatures = true")
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { implicit spark =>
        val featureRDD = ValidatedFeatureRDD(default.featureUris, default.featureType, featureFilter, splitFeatures = true)
        val fireAlertRDD = FireAlertRDD(spark, fireAlert.alertType, fireAlert.alertSource, FeatureFilter.empty)

        val fcdRDD = ForestChangeDiagnosticAnalysis(
          featureRDD,
          fireAlertRDD,
          kwargs
        )

        val fcdDF = ForestChangeDiagnosticDF.getFeatureDataFrame(fcdRDD, spark)
        ForestChangeDiagnosticExport.export(default.featureType, fcdDF, default.outputUrl, kwargs)
      }
    }
  }
}
