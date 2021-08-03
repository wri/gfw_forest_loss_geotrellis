package org.globalforestwatch.summarystats.carbon_sensitivity

import com.monovore.decline.Opts
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._

object CarbonSensitivityCommand extends SummaryCommand {

  val sensitivityTypeOpt: Opts[String] = Opts
    .option[String](
      "sensitivity_type",
      help = "Sensitivity type for carbon flux model"
    )
    .withDefault("standard")

  val carbonSensitivityCommand: Opts[Unit] = Opts.subcommand(
    name = "carbon_sensitivity",
    help = "Compute summary statistics for Carbon Sensitivity Models."
  ) {

    (
      defaultOptions,
      sensitivityTypeOpt,
      defaultFilterOptions,
      gdamFilterOptions
      ).mapN { (default, sensitivityType, defaultFilter, gadmFilter) =>
      val kwargs = Map(
        "outputUrl" -> default._3,
        "splitFeatures" -> default._4,
        "sensitivityType" -> sensitivityType,
        "iso" -> gadmFilter._1,
        "isoFirst" -> gadmFilter._2,
        "isoStart" -> gadmFilter._3,
        "isoEnd" -> gadmFilter._4,
        "admin1" -> gadmFilter._5,
        "admin2" -> gadmFilter._6,
        "limit" -> defaultFilter._1,
        "tcl" -> defaultFilter._2,
        "glad" -> defaultFilter._3
      )

      runAnalysis("carbon_sensitivity", default._1, default._2, kwargs)

    }
  }

}
