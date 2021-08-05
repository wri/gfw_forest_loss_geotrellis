package org.globalforestwatch.summarystats.carbonflux

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts

object CarbonFluxCommand extends SummaryCommand {

  val carbonFluxCommand: Opts[Unit] = Opts.subcommand(
    name = CarbonFluxAnalysis.name,
    help = "Compute summary statistics for GFW dashboards."
  ) {
    (
      defaultOptions,
      defaultFilterOptions,
      gdamFilterOptions,
      wdpaFilterOptions,
      featureFilterOptions
      ).mapN { (default, defaultFilter, gadmFilter, wdpaFilter, featureFilter) =>
      val kwargs = Map(
        "outputUrl" -> default._3,
        "splitFeatures" -> default._4,
        "noOutputPathSuffix" -> default._5,
        "pinnedVersions" -> default._6,
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

      runAnalysis(CarbonFluxAnalysis.name, default._1, default._2, kwargs)

    }
  }
}
