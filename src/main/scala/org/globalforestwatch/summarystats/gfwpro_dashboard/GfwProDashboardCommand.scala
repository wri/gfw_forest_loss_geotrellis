package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._

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

  val gfwProDashboardCommand: Opts[Unit] = Opts.subcommand(
    name = GfwProDashboardAnalysis.name,
    help = "Compute summary statistics for GFW Pro Dashboard."
  ) {
    (
      defaultOptions,
      fireAlertOptions,
      featureFilterOptions,
      contextualFeatureUrlOpt,
      contextualFeatureTypeOpt
      ).mapN { (default, fireAlert, filterOptions, contextualFeatureUrl, contextualFeatureType) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput,
      )
      // TODO: move building the feature object into options
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { spark =>
        val featureRDD = ValidatedFeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures, spark)

        val fireAlertRDD = FireAlertRDD(spark, fireAlert.alertType, fireAlert.alertSource, FeatureFilter.empty)

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
