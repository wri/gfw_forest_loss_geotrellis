package org.globalforestwatch.summarystats.carbon_sensitivity

import com.monovore.decline.Opts
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features._

object CarbonSensitivityCommand extends SummaryCommand {

  val sensitivityTypeOpt: Opts[String] = Opts
    .option[String](
      "sensitivity_type",
      help = "Sensitivity type for carbon flux model"
    )
    .withDefault("standard")

  val carbonSensitivityCommand: Opts[Unit] = Opts.subcommand(
    name = CarbonSensitivityAnalysis.name,
    help = "Compute summary statistics for Carbon Sensitivity Models."
  ) {

    (
      defaultOptions,
      sensitivityTypeOpt,
      featureFilterOptions
      ).mapN { (default, sensitivityType, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "sensitivityType" -> sensitivityType,
        "config" -> GfwConfig.get
      )

      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { spark =>
        val featureRDD = FeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures, spark)
        CarbonSensitivityAnalysis(featureRDD, default.featureType, spark, kwargs)
      }
    }
  }

}
