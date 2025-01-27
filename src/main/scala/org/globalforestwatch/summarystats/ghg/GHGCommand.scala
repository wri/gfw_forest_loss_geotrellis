package org.globalforestwatch.summarystats.ghg

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.features._
import com.typesafe.scalalogging.LazyLogging
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.util.Config
import org.globalforestwatch.util.Util
 
object GHGCommand extends SummaryCommand with LazyLogging {
  // Current range of years to do emissions factors for.
  // Update GHGYearEnd when new tree loss data becomes available.
  val GHGYearStart: Int = 2020
  val GHGYearEnd: Int = 2023

  val backupYieldOpt: Opts[NonEmptyList[String]] = Opts
    .options[String](
      "backup_yield_url",
      help = "URI of backup yield-by-gadm2 area file, in CSV format"
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
        // Read in the backup yield file. Then arrange to broadcast a copy to
        // each node once, rather than copying into each task. The broadcast makes
        // sense (as opposed to a rasterization or spatial partitioning), because the
        // file is currently less than 26 megabytes.
        println(s"Reading backup yield file: ${backupYieldUrl.toList(0)}")
        val backupArray = Util.readFile(backupYieldUrl.toList(0))
        //val backupDF = spark.read
        //  .options(Map("header" -> "true", "delimiter" -> ",", "escape" -> "\""))
        //  .csv(backupYieldUrl.toList: _*)
        val broadcastArray = spark.sparkContext.broadcast(backupArray)

        // We use the "gfwpro_ext" feature id, which includes the extra "commodity"
        // and "yield" columns provided in the input feature file.
        val featureRDD = ValidatedFeatureRDD(default.featureUris, "gfwpro_ext", featureFilter, splitFeatures = true)

        // We set the option to include the featureId in the kwargs passed to the
        // polygonalSummary for each feature. We need the commodity and yield columns
        // in the featureId to determine the yield to use for each pixel.
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
