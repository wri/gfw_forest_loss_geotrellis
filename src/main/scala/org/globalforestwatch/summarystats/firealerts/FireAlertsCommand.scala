package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts

object FireAlertsCommand extends SummaryCommand {
  val changeOnlyOpt: Opts[Boolean] =
    Opts.flag("change_only", "Process change only").orFalse

  val fireAlertsCommand: Opts[Unit] = Opts.subcommand(
    name = FireAlertsAnalysis.name,
    help = "Compute summary fire alert statistics for GFW dashboards."
  ) {
    (
      defaultOptions,
      changeOnlyOpt,
      fireAlertOptions,
      defaultFilterOptions,
      gdamFilterOptions,
      wdpaFilterOptions,
      featureFilterOptions,
      ).mapN {
      (default,
       changeOnly,
       fireAlert,
       defaultFilter,
       gadmFilter,
       wdpaFilter,
       featureFilter) =>
        val kwargs = Map(
          "featureUris" -> default._2,
          "outputUrl" -> default._3,
          "splitFeatures" -> default._4,
          "noOutputPathSuffix" -> default._5,
          "pinnedVersions" -> default._6,
          "changeOnly" -> changeOnly,
          "fireAlertType" -> fireAlert._1,
          "fireAlertSource" -> fireAlert._2,
          "iso" -> gadmFilter._1,
          "isoFirst" -> gadmFilter._2,
          "isoStart" -> gadmFilter._3,
          "isoEnd" -> gadmFilter._4,
          "admin1" -> gadmFilter._5,
          "admin2" -> gadmFilter._6,
          "idStart" -> featureFilter._1,
          "idEnd" -> featureFilter._2,
          "wdpaStatus" -> wdpaFilter._1,
          "iucnCat" -> wdpaFilter._2,
          "limit" -> defaultFilter._1,
          "tcl" -> defaultFilter._2,
          "glad" -> defaultFilter._3
        )

        runAnalysis(FireAlertsAnalysis.name, default._1, default._2, kwargs)
    }
  }

}
