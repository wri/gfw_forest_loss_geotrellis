package org.globalforestwatch.summarystats.integrated_alerts

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features.{FeatureFilter, FeatureRDD}
import org.globalforestwatch.summarystats.gladalerts.GladAlertsAnalysis
import org.globalforestwatch.summarystats.gladalerts.GladAlertsCommand.runAnalysis

object IntegratedAlertsCommand extends SummaryCommand {

  val changeOnlyOpt: Opts[Boolean] =
    Opts.flag("change_only", "Process change only").orFalse

  val integratedAlertsCommand: Opts[Unit] = Opts.subcommand(
    name = IntegratedAlertsAnalysis.name,
    help = "Compute Integrated Alerts summary statistics for GFW dashboards."
  ) {
    (
      defaultOptions,
      featureFilterOptions,
      ).mapN {
      (default,
       filterOptions) =>
        val kwargs = Map(
          "outputUrl" -> default.outputUrl,
          "noOutputPathSuffix" -> default.noOutputPathSuffix,
          "config" -> GfwConfig.get(),
        )

        val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

        runAnalysis { spark =>
          val featureRDD = FeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures, spark)
          IntegratedAlertsAnalysis(featureRDD, default.featureType, spark, kwargs)
        }
    }
  }
}
