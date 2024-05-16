package org.globalforestwatch.summarystats.gladalerts

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features._

object GladAlertsCommand extends SummaryCommand {

  val changeOnlyOpt: Opts[Boolean] =
    Opts.flag("change_only", "Process change only").orFalse

  val gladAlertsCommand: Opts[Unit] = Opts.subcommand(
    name = GladAlertsAnalysis.name,
    help = "Compute GLAD summary statistics for GFW dashboards."
  ) {
    (
      defaultOptions,
      changeOnlyOpt,
      featureFilterOptions
    ).mapN { (default, changeOnly, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "changeOnly" -> changeOnly,
        "config" -> GfwConfig.get()
      )

      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { spark =>
        val featureRDD = FeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures, spark)
        GladAlertsAnalysis(featureRDD, default.featureType, spark, kwargs)
      }
    }
  }
}
