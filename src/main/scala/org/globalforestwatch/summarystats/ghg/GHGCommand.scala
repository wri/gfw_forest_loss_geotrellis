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

  val backupYieldOpt: Opts[NonEmptyList[String]] = Opts
    .options[String](
      "backup_yield_url",
      help = "URI of GADM features in TSV format"
    )

  val ghgCommand: Opts[Unit] = Opts.subcommand(
    name = GHGAnalysis.name,
    help = "Compute greenhouse gas emissions factors for GFW Pro locations."
  ) {
    (
      defaultOptions,
      featureFilterOptions,
      backupYieldOpt
    ).mapN { (default, filterOptions, backupYieldUrl) =>
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
        println("Starting read")
        // Read in the backup yield file. Then we're arranging to broadcast a copy to
        // each node once, rather than copying into each task. The broadcast makes
        // sense (as opposed to a rasterization or spatial partitioning), because the
        // file is currently only 26 megabytes.
        val backupDF = spark.read
          .options(Map("header" -> "true", "delimiter" -> ",", "escape" -> "\""))
          .csv(backupYieldUrl.toList: _*)
        backupDF.printSchema()
        val broadcastArray = spark.sparkContext.broadcast(backupDF.collect())
        println(s"Done read")
        val featureRDD = ValidatedFeatureRDD(default.featureUris, "gfwpro_ext", featureFilter, splitFeatures = true)

        // We add the option to include the featureId in the kwargs passed to the
        // polygonalSummary for each feature. This allows use to use the commodity
        // and yield columns to determine the yield to use for each pixel.
        val fcdRDD = GHGAnalysis(
          featureRDD,
          kwargs + ("backupYield" -> broadcastArray) + ("includeFeatureId" -> true) 
        )

        val fcdDF = GHGDF.getFeatureDataFrame(fcdRDD, spark)
        GHGExport.export(default.featureType, fcdDF, default.outputUrl, kwargs)
      }
    }
  }
}