package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList
import org.globalforestwatch.summarystats.SummaryCommand
import org.globalforestwatch.config.GfwConfig
import cats.implicits._
import com.monovore.decline.Opts
import org.globalforestwatch.summarystats.SummaryAnalysis
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features._
import org.locationtech.jts.geom.Geometry
import com.typesafe.scalalogging.LazyLogging

object ForestChangeDiagnosticCommand extends SummaryCommand with LazyLogging {


  val intermediateListSourceOpt: Opts[Option[NonEmptyList[String]]] = Opts
    .options[String](
      "intermediate_list_source",
      help = "URI of intermediate list results in TSV format"
    ).orNone

  val forestChangeDiagnosticCommand: Opts[Unit] = Opts.subcommand(
    name = ForestChangeDiagnosticAnalysis.name,
    help = "Compute summary statistics for GFW Pro Forest Change Diagnostic."
  ) {
    (
      defaultOptions,
      intermediateListSourceOpt,
      fireAlertOptions,
      featureFilterOptions
      ).mapN { (default, intermediateListSource, fireAlert, filterOptions) =>
      val kwargs = Map(
        "outputUrl" -> default.outputUrl,
        "noOutputPathSuffix" -> default.noOutputPathSuffix,
        "overwriteOutput" -> default.overwriteOutput,
      )

      if (! default.splitFeatures) logger.warn("Forcing splitFeatures = true")
      val featureFilter = FeatureFilter.fromOptions(default.featureType, filterOptions)

      runAnalysis { implicit spark =>
        val featureRDD = FeatureRDD(default.featureUris, default.featureType, featureFilter, splitFeatures = true, spark)
        val fireAlertRDD = FireAlertRDD(spark, fireAlert.alertType, fireAlert.alertSource, FeatureFilter.empty)
        ForestChangeDiagnosticAnalysis(featureRDD, default.featureType, intermediateListSource, fireAlertRDD, kwargs)
      }
    }
  }
}
