package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.GridType

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
      fireAlertOptions,
      featureFilterOptions
    ).mapN { (default, changeOnly, fireAlert, filterOptions) =>
      val kwargs = Map(
        "featureUris" -> default.featureUris,
        "outputUrl" -> default.outputUrl,
        "splitFeatures" -> default.splitFeatures,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "changeOnly" -> changeOnly,
        "fireAlertType" -> fireAlert.alertType
      )

      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { spark =>
        val featureRDD = fireAlert.alertType match {
          case "viirs" | "modis" =>
            val fireRDD = FireAlertRDD(spark, fireAlert.alertType, fireAlert.alertSource, FeatureFilter.empty)
            fireRDD.spatialPartitioning(GridType.QUADTREE)
            FeatureRDD.pointInPolygonJoinAsFeature(fireAlert.alertType, fireRDD)
          case "burned_areas" =>
            val burnedAreasUris = fireAlert.alertSource
            FeatureRDD(
              fireAlert.alertType,
              burnedAreasUris,
              ",",
              default.featureType,
              default.featureUris,
              "\t",
              featureFilter,
              spark
            )
        }

        FireAlertsAnalysis(
          featureRDD,
          default.featureType,
          FeatureFilter.empty,
          spark,
          kwargs
        )
      }
    }
  }

}
