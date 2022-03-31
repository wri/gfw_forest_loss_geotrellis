package org.globalforestwatch.summarystats.carbonflux

import com.monovore.decline.Opts
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import org.globalforestwatch.config.RasterCatalog
import org.globalforestwatch.features._

object CarbonFluxCommand extends SummaryCommand {



  val carbonFluxCommand: Opts[Unit] = Opts.subcommand(
    name = CarbonFluxAnalysis.name,
    help = "Compute forest carbon flux model statistics with contextual layers of particular interest to the model."
  ) {
    (defaultOptions, featureFilterOptions).mapN { (default, filterOptions) =>
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
