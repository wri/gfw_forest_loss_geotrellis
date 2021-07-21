package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts

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
      defaultFilterOptions,
      featureFilterOptions,
      gdamFilterOptions,
      contextualFeatureUrlOpt,
      contextualFeatureTypeOpt
      ).mapN { (default, fireAlert, defaultFilter, featureFilter, gadmFilter, contextualFeatureUrl, contextualFeatureType) =>
      val kwargs = Map(
        "featureUris" -> default._2,
        "outputUrl" -> default._3,
        "splitFeatures" -> default._4,
        "noOutputPathSuffix" -> default._5,
        "pinnedVersions" -> default._6,
        "fireAlertType" -> fireAlert._1,
        "fireAlertSource" -> fireAlert._2,
        "idStart" -> featureFilter._1,
        "idEnd" -> featureFilter._2,
        "limit" -> defaultFilter._1,
        "tcl" -> defaultFilter._2,
        "glad" -> defaultFilter._3,
        "contextualFeatureUrl" -> contextualFeatureUrl,
        "contextualFeatureType" -> contextualFeatureType,
        "iso" -> gadmFilter._1,
        "isoFirst" -> gadmFilter._2,
        "isoStart" -> gadmFilter._3,
        "isoEnd" -> gadmFilter._4,
        "admin1" -> gadmFilter._5,
        "admin2" -> gadmFilter._6,
      )

      runAnalysis(GfwProDashboardAnalysis.name, default._1, default._2, kwargs)

    }
  }
}
