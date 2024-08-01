package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import cats.implicits._
import com.monovore.decline.Opts
import geotrellis.vector.Geometry
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.features._
import org.locationtech.jts.geom.Geometry

object GfwProDashboardCommand extends SummaryCommand {

  val gadmFeatureUrl: Opts[NonEmptyList[String]] = Opts
    .options[String](
      "gadm_feature_url",
      help = "URI of GADM features in TSV format"
    )

  val gadmIntersectThreshold: Opts[Int] = Opts
    .option[Int](
      "gadm_intersect_threshold",
      help = "Number of input features at which to intersect GADM"
    ).withDefault(50)

  val gfwProDashboardCommand: Opts[Unit] = Opts.subcommand(
    name = GfwProDashboardAnalysis.name,
    help = "Compute summary statistics for GFW Pro Dashboard."
  ) {
    (
      defaultOptions,
      optionalFireAlertOptions,
      featureFilterOptions,
      gadmFeatureUrl,
      gadmIntersectThreshold,
      pinnedVersionsOpts
      ).mapN { (default, fireAlert, filterOptions, gadmFeatureUrl, gadmIntersectThreshold, pinned) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput,
        "config" -> GfwConfig.get(pinned)
      )
      // TODO: move building the feature object into options
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { implicit spark =>
        val featureRDD = ValidatedFeatureRDD(default.featureUris, default.featureType, featureFilter, default.splitFeatures, gfwProAddCentroid = true)

        val fireAlertRDD = fireAlert.alertSource match {
          case Some(alertSource) =>
            FireAlertRDD(spark, fireAlert.alertType, alertSource, FeatureFilter.empty)
          case None =>
            // If no sources provided, just create an empty RDD
            val spatialRDD = new SpatialRDD[Geometry]
            spatialRDD.rawSpatialRDD = spark.sparkContext.emptyRDD[Geometry].toJavaRDD()
            spatialRDD
        }

        val featureCount = featureRDD.count()
        val doGadmIntersect = featureCount > gadmIntersectThreshold
        if (doGadmIntersect) {
          println(s"Intersecting vector gadm for feature count $featureCount")
        } else {
          println(s"Using raster gadm for feature count $featureCount")
        }
          
        val dashRDD = GfwProDashboardAnalysis(
          featureRDD,
          default.featureType,
          doGadmIntersect,
          gadmFeatureUrl,
          fireAlertRDD,
          spark,
          kwargs
        )
        val summaryDF = GfwProDashboardDF.getFeatureDataFrameFromVerifiedRdd(dashRDD.unify, spark)
        GfwProDashboardExport.export(default.featureType, summaryDF, default.outputUrl, kwargs)
      }
    }
  }
}
