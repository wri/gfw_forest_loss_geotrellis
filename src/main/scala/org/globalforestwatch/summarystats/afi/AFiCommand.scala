package org.globalforestwatch.summarystats.afi

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import geotrellis.vector.Geometry
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats.afi.AFiAnalysis.getOutputUrl
import org.locationtech.jts.geom.Geometry
import cats.data.Validated.Valid


object AFiCommand extends SummaryCommand {

  val afiCommand: Opts[Unit] = Opts.subcommand(
    name = AFiAnalysis.name,
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
        "config" -> GfwConfig.get()
      )
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { implicit spark =>
        val featureRDD = ValidatedFeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures)
        val filteredFeatureRDD = featureRDD.filter{
          case Valid((GfwProFeatureId(_, locationId), _)) => locationId != -2
          case _ => true
        }

        AFiAnalysis(
          filteredFeatureRDD,
          default.featureType,
          spark,
          kwargs
        )

        val runOutputUrl: String = getOutputUrl(kwargs)
        AFiExport.export(default.featureType, resultsDF, runOutputUrl, kwargs)
      }
    }
  )
}
