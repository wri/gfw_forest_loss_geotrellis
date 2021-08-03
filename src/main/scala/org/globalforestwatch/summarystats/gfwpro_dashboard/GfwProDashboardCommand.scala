package org.globalforestwatch.summarystats.gfwpro_dashboard

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts

object GfwProDashboardCommand extends SummaryCommand {

  val gfwProDashboardCommand: Opts[Unit] = Opts.subcommand(
    name = "gfwpro_dashboard",
    help = "Compute summary statistics for GFW Pro Dashboard."
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
        "splitFeatures" -> default._4,
        "fireAlertType" -> fireAlert._1,
        "fireAlertSource" -> fireAlert._2,
        "idStart" -> featureFilter._1,
        "idEnd" -> featureFilter._2,
        "limit" -> defaultFilter._1,
        "tcl" -> defaultFilter._2,
        "glad" -> defaultFilter._3
      )

      runAnalysis("gfwpro_dashboard", default._1, default._2, kwargs)

    }
  }
}
