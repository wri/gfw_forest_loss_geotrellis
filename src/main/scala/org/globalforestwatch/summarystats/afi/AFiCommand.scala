package org.globalforestwatch.summarystats.afi

import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats.afi.AFiAnalysis.getOutputUrl
import cats.data.Validated.Valid
import org.globalforestwatch.util.Config
import cats.data.NonEmptyList

object AFiCommand extends SummaryCommand {
  // Current range of years for UMD tree cover loss to include and break out during AFi analysis.
  val TreeCoverLossYearStart: Int = 2021
  val TreeCoverLossYearEnd: Int = 2023

  val afiCommand: Opts[Unit] = Opts.subcommand(
    name = AFiAnalysis.name,
    help = "Compute summary statistics for GFW Pro Dashboard."
  ) (
    (
      defaultOptions,
      featureFilterOptions,
      gadmVersOpt
      ).mapN { (default, filterOptions, gadmVersOpt) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput,
        // Pin the version of gfw_integrated_alerts, so we don't make a data API request for 'latest'
        "config" -> GfwConfig.get(Some(NonEmptyList.one(Config("gfw_integrated_alerts", "v20231121")))),
        "gadmVers" -> gadmVersOpt
      )
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { implicit spark =>
        val featureRDD = ValidatedFeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures)
        val filteredFeatureRDD = featureRDD.filter{
          case Valid((GfwProFeatureId(_, locationId), _)) => locationId != -2
          case _ => true
        }

        val resultsDF = AFiAnalysis(
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
