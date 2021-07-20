package org.globalforestwatch.summarystats.annualupdate_minimal

import com.monovore.decline.Opts
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._

object AnnualUpdateMinimalCommand extends SummaryCommand {

  val annualupdateMinimalCommand: Opts[Unit] = Opts.subcommand(
    name = AnnualUpdateMinimalAnalysis.name,
    help = "Compute summary statistics for GFW dashboards."
  ) {
    (
      defaultOptions,
      defaultFilterOptions,
      gdamFilterOptions,
      wdpaFilterOptions,
      featureFilterOptions,
      ).mapN { (default, defaultFilter, gadmFilter, wdpaFilter, featureFilter) =>
      val kwargs = Map(
        "outputUrl" -> default._3,
        "splitFeatures" -> default._4,
        "noOutputPathSuffix" -> default._5,
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

      runAnalysis(
        AnnualUpdateMinimalAnalysis.name,
        default._1,
        default._2,
        kwargs
      )

    }
  }
}
