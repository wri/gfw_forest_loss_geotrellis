package org.globalforestwatch.summarystats

import cats.data.NonEmptyList
import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.Logger
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.globalforestwatch.features._

object SummaryMain
    extends CommandApp(
      name = "geotrellis-summary-stats",
      header = "Compute summary statistics for GFW data",
      main = {

        val analysisOpt =
          Opts.option[String]("analysis", "Type of analysis to run")

        val featuresOpt =
          Opts.options[String]("features", "URI of features in TSV format")

        val outputOpt =
          Opts.option[String]("output", "URI of output dir for CSV files")

        //        // Can be used to increase the level of job parallelism
        //        val intputPartitionsOpt = Opts
        //          .option[Int]("input.tsv-partitions", "Partition multiplier for input.tsv")
        //          .withDefault(16)
        //
        //        // Can be used to consolidate output into fewer files
        //        val outputPartitionsOpt = Opts
        //          .option[Int](
        //            "output-partitions",
        //            "Number of output partitions / files to be written"
        //          )
        //          .orNone

        val featureTypeOpt = Opts
          .option[String](
            "feature_type",
            help =
              "Feature type: one of 'gadm', 'wdpa', 'geostore' or 'feature'"
          )
          .withDefault("feature")

        val limitOpt = Opts
          .option[Int]("limit", help = "Limit number of records processed")
          .orNone

        val isoFirstOpt =
          Opts
            .option[String](
              "iso_first",
              help = "Filter by first letter of ISO code"
            )
            .orNone

        val isoStartOpt =
          Opts
            .option[String](
              "iso_start",
              help = "Filter by ISO code larger than or equal to given value"
            )
            .orNone

        val isoEndOpt =
          Opts
            .option[String](
              "iso_end",
              help = "Filter by ISO code smaller than given value"
            )
            .orNone

        val isoOpt =
          Opts.option[String]("iso", help = "Filter by country ISO code").orNone

        val admin1Opt = Opts
          .option[String]("admin1", help = "Filter by country Admin1 code")
          .orNone

        val admin2Opt = Opts
          .option[String]("admin2", help = "Filter by country Admin2 code")
          .orNone

        val idStartOpt =
          Opts
            .option[Int](
              "id_start",
              help = "Filter by IDs larger than or equal to given value"
            )
            .orNone

        //        val idEndOpt =
        //          Opts
        //            .option[Int](
        //              "id_end",
        //              help = "Filter by IDs smaller than given value"
        //            )
        //            .orNone
        //
        //        val iucnCatOpts =
        //          Opts
        //            .options[String]("iucn_cat", help = "Filter by IUCN Category")
        //            .orNone

        val wdpaStatusOpts =
          Opts
            .options[String]("wdpa_status", help = "Filter by WDPA Status")
            .orNone

        val tcdOpt =
          Opts
            .option[Int]("tcd", help = "Select tree cover density year")
            .withDefault(2000)

        val thresholdOpts = Opts
          .options[Int]("threshold", "Treecover threshold to apply")
          .withDefault(List(30))

        val contextualLayersOpts = Opts
          .options[String]("contextual_layer", "Contextual Layer to include (currently supported: is__umd_regional_primary_forest_2001, is__gfw_plantations")
          .withDefault(NonEmptyList.of(""))

        val tclOpt = Opts.flag("tcl", "TCL tile extent").orFalse

        val gladOpt = Opts.flag("glad", "GLAD tile extent").orFalse

        val changeOnlyOpt =
          Opts.flag("change_only", "Process change only").orFalse

        //        val buildDataCubeOpt =
        //          Opts.flag("build_data_cube", "Build XYZ data cube").orFalse

        val sensitivityTypeOpt = Opts
          .option[String](
            "sensitivity_type",
            help = "Sensitivity type for carbon flux model"
          )
          .withDefault("standard")

        val fireAlertTypeOpt = Opts
          .option[String]("fire_alert_type", help = "MODIS or VIIRS")
          .withDefault("VIIRS")

        val fireAlertSourceOpt = Opts
          .options[String](
            "fire_alert_source",
            help = "URI of fire alerts in TSV format"
          )
          .orNone

        val logger = Logger.getLogger("SummaryMain")

        (
          analysisOpt,
          featuresOpt,
          outputOpt,
          featureTypeOpt,
          limitOpt,
          isoOpt,
          isoFirstOpt,
          isoStartOpt,
          isoEndOpt,
          admin1Opt,
          admin2Opt,
          idStartOpt,


          wdpaStatusOpts,
          tcdOpt,
          thresholdOpts,
          contextualLayersOpts,
          tclOpt,
          gladOpt,
          changeOnlyOpt,
          fireAlertTypeOpt,
          fireAlertSourceOpt,
          sensitivityTypeOpt
//          buildDataCubeOpt
        ).mapN {
          (analysis,
           featureUris,
           outputUrl,
           featureType,
           limit,
           iso,
           isoFirst,
           isoStart,
           isoEnd,
           admin1,
           admin2,
           idStart,
           //           idEnd,
           //           iucnCat,
           wdpaStatus,
           tcdYear,
           thresholdFilter,
           contextualLayers,
           tcl,
           glad,
           changeOnly,
           //           buildDataCube
           fireAlertType,
           fireAlertSources,
           sensitivityType) =>
            val kwargs = Map(
              "outputUrl" -> outputUrl,
              "limit" -> limit,
              "iso" -> iso,
              "isoFirst" -> isoFirst,
              "isoStart" -> isoStart,
              "isoEnd" -> isoEnd,
              "admin1" -> admin1,
              "admin2" -> admin2,
              "idStart" -> idStart,
              //              "idEnd" -> idEnd,
              //              "iucnCat" -> iucnCat,
              "wdpaStatus" -> wdpaStatus,
              "tcdYear" -> tcdYear,
              "thresholdFilter" -> thresholdFilter,
              "contextualLayers" -> contextualLayers,
              "tcl" -> tcl,
              "glad" -> glad,
              "changeOnly" -> changeOnly,
//              "buildDataCube" -> buildDataCube
              "fireAlertType" -> fireAlertType,
              "fireAlertSource" -> fireAlertSources,
              "featureUris" -> featureUris,
              "sensitivityType" -> sensitivityType
            )

            val spark: SparkSession =
              SummarySparkSession("GFW Summary Stats Spark Session")
            //import spark.implicits._

            /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
            val featureRDD: RDD[Feature[Geometry, FeatureId]] =
              FeatureRDDFactory(
                analysis,
                featureType,
                featureUris,
                kwargs,
                spark
              )

            SummaryAnalysisFactory(
              analysis,
              featureRDD,
              featureType,
              spark,
              kwargs
            ).runAnalysis

            spark.stop
        }
      }
    )
