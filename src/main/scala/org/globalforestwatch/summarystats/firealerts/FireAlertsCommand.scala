package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts

object FireAlertsCommand extends SummaryCommand {

  val fireAlertsCommand: Opts[Unit] = Opts.subcommand(
    name = "firealerts",
    help = "Compute summary fire alert statistics for GFW dashboards."
  ) {
    (
      defaultOptions,
      fireAlertOptions,
      defaultFilterOptions,
      gdamFilterOptions,
      wdpaFilterOptions,
      featureFilterOptions,
      ).mapN {
      (default,
       fireAlert,
       defaultFilter,
       gadmFilter,
       wdpaFilter,
       featureFilter) =>
        val kwargs = Map(
          "featureUris" -> default._2,
          "outputUrl" -> default._3,
          "splitFeatures" -> default._4,
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

        runAnalysis("firealerts", default._1, default._2, kwargs)

    }
  }

}
