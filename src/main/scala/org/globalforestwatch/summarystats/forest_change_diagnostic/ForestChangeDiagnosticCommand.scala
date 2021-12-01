package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._
import com.typesafe.scalalogging.LazyLogging

object ForestChangeDiagnosticCommand extends SummaryCommand with LazyLogging {


  val intermediateListSourceOpt: Opts[Option[NonEmptyList[String]]] = Opts
    .options[String](
      "intermediate_list_source",
      help = "URI of intermediate list results in TSV format"
    ).orNone

  val forestChangeDiagnosticCommand: Opts[Unit] = Opts.subcommand(
    name = ForestChangeDiagnosticAnalysis.name,
    help = "Compute summary statistics for GFW Pro Forest Change Diagnostic."
  ) {
    (
      defaultOptions,
      intermediateListSourceOpt,
      fireAlertOptions,
      featureFilterOptions
      ).mapN { (default, intermediateListSource, fireAlert, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput
      )

      if (! default.splitFeatures) logger.warn("Forcing splitFeatures = true")
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { implicit spark =>
        val featureRDD = ValidatedFeatureRDD(default.featureUris, default.featureType, featureFilter, splitFeatures = true, spark)
        val fireAlertRDD = FireAlertRDD(spark, fireAlert.alertType, fireAlert.alertSource, FeatureFilter.empty)
        ForestChangeDiagnosticAnalysis(default.featureType, featureRDD, intermediateListSource, fireAlertRDD, default.outputUrl, kwargs)
      }
    }
  }
}
