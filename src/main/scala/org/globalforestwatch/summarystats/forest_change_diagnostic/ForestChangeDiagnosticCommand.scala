package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts

object ForestChangeDiagnosticCommand extends SummaryCommand {

  val forestChangeDiagnosticCommand: Opts[Unit] = Opts.subcommand(
    name = "forest_change_diagnostic",
    help = "Compute summary statistics for GFW Pro Forest Change Diagnostic."
  ) {
    (
      defaultOptions,
      fireAlertOptions,
      defaultFilterOptions,
      featureFilterOptions
      ).mapN { (default, fireAlert, defaultFilter, featureFilter) =>
      val kwargs = Map(
        "featureUris" -> default._2,
        "outputUrl" -> default._3,
        "splitFeatures" -> true, // force to split features
        "fireAlertType" -> fireAlert._1,
        "fireAlertSource" -> fireAlert._2,
        "idStart" -> featureFilter._1,
        "idEnd" -> featureFilter._2,
        "limit" -> defaultFilter._1,
        "tcl" -> defaultFilter._2,
        "glad" -> defaultFilter._3
      )

      runAnalysis("forest_change_diagnostic", default._1, default._2, kwargs)

    }
  }
}
