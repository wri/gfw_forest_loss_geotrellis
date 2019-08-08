package org.globalforestwatch.treecoverloss

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.globalforestwatch.features.{SimpleFeature, SimpleFeatureId}
import org.globalforestwatch.util.SummarySparkSession

object TreeLossSummaryMain
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

      val tcdOpt =
        Opts
          .option[Int]("tcd", help = "Select tree cover density year")
          .withDefault(2000)

      val thresholdOpts = Opts
        .options[Int]("threshold", "Treecover threshold to apply")
        .orEmpty

      val primartyForestOpt = Opts
        .flag("primary-forests", "Include Primary Forests")
        .orFalse
        .withDefault(false)

      val limitOpt = Opts
        .option[Int]("limit", help = "Limit number of records processed")
        .orNone

      val idStartOpt =
        Opts
          .option[Int](
          "id_start",
          help = "Filter by Feature IDs larger than or equal to given value"
        )
          .orNone

      val idEndOpt =
        Opts
          .option[Int](
          "id_end",
          help = "Filter by Feature IDs smaller than given value"
        )
          .orNone


      val tclOpt = Opts.flag("tcl", "TCL tile extent").orFalse

      val gladOpt = Opts.flag("glad", "GLAD tile extent").orFalse

      val logger = Logger.getLogger("TreeLossSummaryMain")

      (
        featuresOpt,
        outputOpt,
        intputPartitionsOpt,
        outputPartitionsOpt,
        tcdOpt,
        thresholdOpts,
        primartyForestOpt,
        limitOpt,
        idStartOpt,
        idEndOpt,
        tclOpt,
        gladOpt
      ).mapN {
        (featureUris,
         outputUrl,
         inputPartitionMultiplier,
         maybeOutputPartitions,
         tcdYear,
         thresholdFilter,
         includePrimaryForest,
         limit,
         idStart,
         idEnd,
         tcl,
         glad) =>
          val spark: SparkSession = SummarySparkSession("Tree Cover Loss Anlaysis Spark Session")
          import spark.implicits._

          val filters = Map(
            "limit" -> limit,
            "idStart" -> idStart,
            "idEnd" -> idEnd,
            "tcl" -> tcl,
            "glad" -> glad
          )

          // ref: https://github.com/databricks/spark-csv
          val featuresDF: DataFrame = spark.read
            .options(Map("header" -> "true", "delimiter" -> "\t"))
            .csv(featureUris.toList: _*)
            .transform(
              SimpleFeature.filter(filters)
            )

          /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
          val featureRDD: RDD[Feature[Geometry, SimpleFeatureId]] =
            featuresDF.rdd.mapPartitions({ iter: Iterator[Row] =>
                for {
                  i <- iter
                  if SimpleFeature.isValidGeom(i)
                } yield {
                  SimpleFeature.getFeature(i)
                }
            }, preservesPartitioning = true)

          val part = new HashPartitioner(
            partitions = featureRDD.getNumPartitions * inputPartitionMultiplier
          )

          val summaryRDD: RDD[(SimpleFeatureId, TreeLossSummary)] =
            TreeLossRDD(featureRDD, TreeLossGrid.blockTileGrid, part, tcdYear)

          val summaryDF =
            summaryRDD
              .flatMap {
                case (id, treeLossSummary) =>
                  treeLossSummary.stats.map {
                    case (lossDataGroup, lossData) => {

                      TreeLossRow(
                        id.cell_id,
                        lossDataGroup.threshold,
                        tcdYear,
                          lossDataGroup.primaryForest,
                        lossData.extent2000,
                        lossData.extent2010,
                        lossData.totalArea,
                        lossData.totalGainArea,
                        lossData.totalBiomass,
                        lossData.totalCo2,
                        lossData.biomassHistogram.mean(),
                        TreeLossYearDataMap.toList(lossData.lossYear)
                      )
                    }
                  }
              }
              .toDF(
                "feature_id",
                "threshold",
                "tcd_year",
                "primary_forest",
                "extent_2000",
                "extent_2010",
                "total_area",
                "total_gain",
                "total_biomass",
                "total_co2",
                "avg_biomass_per_ha",
                "year_data"
              )

          val runOutputUrl = outputUrl + "/treecoverloss_" +
            DateTimeFormatter
              .ofPattern("yyyyMMdd_HHmm")
              .format(LocalDateTime.now)

          val outputPartitionCount =
            maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

          val csvOptions = Map(
            "header" -> "true",
            "delimiter" -> "\t",
            "quote" -> "\u0000",
            "quoteMode" -> "NONE",
            "nullValue" -> "\u0000"
          )

          //          val tcdFilter: List[Int] = thresholdFilter

          summaryDF
            .filter($"threshold".isin(thresholdFilter: _*))
            //              .repartition($"feature_id", $"threshold")
            .transform(DF.unpackValues)
            .transform(DF.primaryForestFilter(includePrimaryForest))
            .coalesce(1)
            .orderBy($"feature_id", $"threshold")
            .write
            .options(csvOptions)
            .csv(path = runOutputUrl)

          spark.stop
      }
    }
  )
