package org.globalforestwatch.gladalerts

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.globalforestwatch.features._
import org.globalforestwatch.util.SummarySparkSession

object GladAlertsSummaryMain
    extends CommandApp(
      name = "geotrellis-tree-cover-loss",
      header = "Compute statistics on tree cover loss",
      main = {

        val featuresOpt =
          Opts.options[String]("features", "URI of features in TSV format")

        val outputOpt =
          Opts.option[String]("output", "URI of output dir for CSV files")

        // Can be used to increase the level of job parallelism
        val intputPartitionsOpt = Opts
          .option[Int]("input-partitions", "Partition multiplier for input")
          .withDefault(16)

        // Can be used to consolidate output into fewer files
        val outputPartitionsOpt = Opts
          .option[Int](
            "output-partitions",
            "Number of output partitions / files to be written"
          )
          .orNone

        val featureTypeOpt = Opts
          .option[String](
          "feature_type",
          help = "Feature type: Simple feature or GADM"
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

        val wdpaIdStartOpt =
          Opts
            .option[Int](
            "wdpaid_start",
            help = "Filter by WDPA IDs larger than or equal to given value"
          )
            .orNone

        val wdpaIdEndOpt =
          Opts
            .option[Int](
            "wdpaid_end",
            help = "Filter by WDPA IDs smaller than given value"
          )
            .orNone

        val iucnCatOpts =
          Opts
            .options[String]("iucn_cat", help = "Filter by IUCN Category")
            .orNone

        val wdpaStatusOpts =
          Opts
            .options[String]("wdpa_status", help = "Filter by WDPA Status")
            .orNone

        val tclOpt = Opts.flag("tcl", "TCL tile extent").orFalse

        val gladOpt = Opts.flag("glad", "GLAD tile extent").orFalse

        val logger = Logger.getLogger("TreeLossSummaryMain")

        (
          featuresOpt,
          outputOpt,
          intputPartitionsOpt,
          outputPartitionsOpt,
          featureTypeOpt,
          limitOpt,
          isoOpt,
          isoFirstOpt,
          isoStartOpt,
          isoEndOpt,
          admin1Opt,
          admin2Opt,
          wdpaIdStartOpt,
          wdpaIdEndOpt,
          iucnCatOpts,
          wdpaStatusOpts,
          tclOpt,
          gladOpt
        ).mapN {
          (featureUris,
           outputUrl,
           inputPartitionMultiplier,
           maybeOutputPartitions,
           featureType,
           limit,
           iso,
           isoFirst,
           isoStart,
           isoEnd,
           admin1,
           admin2,
           wdpaIdStart,
           wdpaIdEnd,
           iucnCat,
           wdpaStatus,
           tcl,
           glad) =>
            val spark: SparkSession =
              SummarySparkSession("Glad Alerts Analysis Spark Session")
            import spark.implicits._

            val filters = Map(
              "limit" -> limit,
              "iso" -> iso,
              "isoFirst" -> isoFirst,
              "isoStart" -> isoStart,
              "isoEnd" -> isoEnd,
              "admin1" -> admin1,
              "admin2" -> admin2,
              "wdpaIdStart" -> wdpaIdStart,
              "wdpaIdEnd" -> wdpaIdEnd,
              "iucnCat" -> iucnCat,
              "wdpaStatus" -> wdpaStatus,
              "tcl" -> tcl,
              "glad" -> glad
            )

            val featureObj = FeatureFactory(featureType).featureObj
            //            type featureId = FeatureIdFactory(featureType)

            // ref: https://github.com/databricks/spark-csv
            val featuresDF: DataFrame = spark.read
              .options(Map("header" -> "true", "delimiter" -> "\t"))
              .csv(featureUris.toList: _*)
              .transform(featureObj.filter(filters))

            /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
            val featureRDD: RDD[Feature[Geometry, FeatureId]] =
              featuresDF.rdd.mapPartitions({ iter: Iterator[Row] =>
                for {
                  i <- iter
                  if featureObj.isValidGeom(i)
                } yield {
                  featureObj.getFeature(i)
                }
              }, preservesPartitioning = true)

            val part = new HashPartitioner(
              partitions = featureRDD.getNumPartitions * inputPartitionMultiplier
            )

            val summaryRDD: RDD[(FeatureId, GladAlertsSummary)] =
              GladAlertsRDD(featureRDD, GladAlertsGrid.blockTileGrid, part)

            val summaryDF = GladAlertsSummaryDFFactory(featureType, summaryRDD, spark).getDataFrame

            val outputPartitionCount =
              maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

            summaryDF.repartition(partitionExprs = $"id")

            GladAlertsExport.export(featureType, summaryDF, outputUrl)

            spark.stop
        }
      }
    )
