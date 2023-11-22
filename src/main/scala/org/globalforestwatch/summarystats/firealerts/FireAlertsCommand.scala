package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._
import org.apache.sedona.core.enums.GridType
import org.globalforestwatch.config.GfwConfig

object FireAlertsCommand extends SummaryCommand {
  val changeOnlyOpt: Opts[Boolean] =
    Opts.flag("change_only", "Process change only").orFalse

  val fireAlertsCommand: Opts[Unit] = Opts.subcommand(
    name = FireAlertsAnalysis.name,
    help = "Compute summary fire alert statistics for GFW dashboards."
  ) {
    (
      defaultOptions,
      changeOnlyOpt,
      requiredFireAlertOptions,
      featureFilterOptions
    ).mapN { (default, changeOnly, fireAlert, filterOptions) =>
      val kwargs = Map(
        "featureUris" -> default.featureUris,
        "outputUrl" -> default.outputUrl,
        "splitFeatures" -> default.splitFeatures,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "changeOnly" -> changeOnly,
        "fireAlertType" -> fireAlert.alertType,
        "config" -> GfwConfig.get(None)
      )

      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)
      val firesFeatureFilter = FeatureFilter.fromOptions(fireAlert.alertType, filterOptions)

      runAnalysis { spark =>
        val featureRDD = fireAlert.alertType match {
          case "viirs" | "modis" =>
            FeatureRDD(fireAlert.alertSource, fireAlert.alertType, firesFeatureFilter, false, spark)
          case "burned_areas" =>
            val burnedAreasUris = fireAlert.alertSource
            FeatureRDD(
              fireAlert.alertType,
              burnedAreasUris,
              ",",
              default.featureType,
              default.featureUris,
              "\t",
              firesFeatureFilter,
              featureFilter,
              spark
            )
        }

        FireAlertsAnalysis(
          featureRDD,
          default.featureType,
          featureFilter,
          spark,
          kwargs
        )
      }
    }
  }

}
