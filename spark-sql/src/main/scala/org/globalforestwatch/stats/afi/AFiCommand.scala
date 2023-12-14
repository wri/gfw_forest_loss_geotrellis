package org.globalforestwatch.stats.afi

import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.command.SparkCommand
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats.SummaryCommand

object AFiCommand extends SparkCommand with SummaryCommand {
  val command: Opts[Unit] = Opts.subcommand(
    name = "afi",
    help = "Compute summary statistics for GFW Pro Dashboard."
  ) (
    (
      defaultOptions,
      featureFilterOptions,
    ).mapN { (default, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput,
        "config" -> GfwConfig.get
      )
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      withSpark { implicit spark =>
        AFiAnalysis(
          default.featureUris,
          default.featureType,
          featureFilter,
          kwargs
        )
      }
    }
  )
}
