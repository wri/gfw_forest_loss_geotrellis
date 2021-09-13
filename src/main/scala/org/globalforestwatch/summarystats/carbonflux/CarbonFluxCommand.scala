package org.globalforestwatch.summarystats.carbonflux

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._

object CarbonFluxCommand extends SummaryCommand {

  val carbonFluxCommand: Opts[Unit] = Opts.subcommand(
    name = CarbonFluxAnalysis.name,
    help = "Compute summary statistics for GFW dashboards."
  ) {
    (
      defaultOptions,
      featureFilterOptions
      ).mapN { (default, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix
      )

      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { spark =>
        val featureRDD = FeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures, spark)
        CarbonFluxAnalysis(featureRDD, default.featureType, spark, kwargs)
      }
    }
  }
}
