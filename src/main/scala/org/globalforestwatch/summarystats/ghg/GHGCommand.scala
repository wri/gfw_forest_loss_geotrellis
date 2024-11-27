package org.globalforestwatch.summarystats.ghg

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._
import com.typesafe.scalalogging.LazyLogging
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.util.Config

object GHGCommand extends SummaryCommand with LazyLogging {
  // Current range of years to do emissions factors for.
  // Update GHGYearEnd when new tree loss data becomes available.
  val GHGYearStart: Int = 2020
  val GHGYearEnd: Int = 2023

  val ghgCommand: Opts[Unit] = Opts.subcommand(
    name = GHGAnalysis.name,
    help = "Compute greenhouse gas emissions factors for GFW Pro locations."
  ) {
    (
      defaultOptions,
      featureFilterOptions
    ).mapN { (default, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput,
        // Pin the version of gfw_integrated_alerts, so we don't make a data API request for 'latest'
        "config" -> GfwConfig.get(Some(NonEmptyList.one(Config("gfw_integrated_alerts", "v20231121"))))
      )

      if (!default.splitFeatures) logger.warn("Forcing splitFeatures = true")
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { implicit spark =>
        val featureRDD = ValidatedFeatureRDD(default.featureUris, default.featureType, featureFilter, splitFeatures = true)

        val fcdRDD = GHGAnalysis(
          featureRDD,
          kwargs
        )

        val fcdDF = GHGDF.getFeatureDataFrame(fcdRDD, spark)
        GHGExport.export(default.featureType, fcdDF, default.outputUrl, kwargs)
      }
    }
  }
}
