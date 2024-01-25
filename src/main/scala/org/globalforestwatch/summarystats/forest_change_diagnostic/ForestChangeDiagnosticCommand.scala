package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._
import com.typesafe.scalalogging.LazyLogging
import org.globalforestwatch.summarystats.ValidatedLocation
import org.apache.spark.rdd.RDD
import org.globalforestwatch.config.GfwConfig

object ForestChangeDiagnosticCommand extends SummaryCommand with LazyLogging {
  // Current range of years for UMD tree cover loss and country-specific tree cover loss.
  // Update TreeCoverLossYearEnd when new data becomes available.
  val TreeCoverLossYearStart: Int = 2001
  val TreeCoverLossYearEnd: Int = 2022

  val intermediateListSourceOpt: Opts[Option[NonEmptyList[String]]] = Opts
    .options[String](
      "intermediate_list_source",
      help = "URI of intermediate list results in TSV format"
    )
    .orNone

  val forestChangeDiagnosticCommand: Opts[Unit] = Opts.subcommand(
    name = ForestChangeDiagnosticAnalysis.name,
    help = "Compute summary statistics for GFW Pro Forest Change Diagnostic."
  ) {
    (
      defaultOptions,
      intermediateListSourceOpt,
      requiredFireAlertOptions,
      featureFilterOptions
    ).mapN { (default, intermediateListSource, fireAlert, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput,
        "config" -> GfwConfig.get()
      )

      if (!default.splitFeatures) logger.warn("Forcing splitFeatures = true")
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { implicit spark =>
        val featureRDD = ValidatedFeatureRDD(default.featureUris, default.featureType, featureFilter, splitFeatures = true)
        val fireAlertRDD = FireAlertRDD(spark, fireAlert.alertType, fireAlert.alertSource, FeatureFilter.empty)

        val intermediateResultsRDD = intermediateListSource.map { sources =>
          ForestChangeDiagnosticDF.readIntermidateRDD(sources, spark)
        }
        val saveIntermidateResults: RDD[ValidatedLocation[ForestChangeDiagnosticData]] => Unit = { rdd =>
          val df = ForestChangeDiagnosticDF.getGridFeatureDataFrame(rdd, spark)
          ForestChangeDiagnosticExport.export("intermediate", df, default.outputUrl, kwargs)
        }

        val fcdRDD = ForestChangeDiagnosticAnalysis(
          featureRDD,
          intermediateResultsRDD,
          fireAlertRDD,
          saveIntermidateResults,
          kwargs
        )

        val fcdDF = ForestChangeDiagnosticDF.getFeatureDataFrame(fcdRDD, spark)
        ForestChangeDiagnosticExport.export(default.featureType, fcdDF, default.outputUrl, kwargs)
      }
    }
  }
}
