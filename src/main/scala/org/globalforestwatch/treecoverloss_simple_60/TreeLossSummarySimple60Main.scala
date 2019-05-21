package org.globalforestwatch.treecoverloss_simple_60

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.io.wkb.WKB
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._

object TreeLossSummarySimple60Main
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

        val limitOpt = Opts
          .option[Int]("limit", help = "Limit number of records processed")
          .orNone

        val logger = Logger.getLogger("TreeLossSummaryMain")

        (
          featuresOpt,
          outputOpt,
          intputPartitionsOpt,
          outputPartitionsOpt,
          limitOpt
        ).mapN {
          (featureUris,
           outputUrl,
           inputPartitionMultiplier,
           maybeOutputPartitions,
           limit) =>
            val spark: SparkSession = TreeLossSparkSession()

            import spark.implicits._

            // ref: https://github.com/databricks/spark-csv
            var featuresDF: DataFrame = spark.read
              .options(Map("header" -> "true", "delimiter" -> "\t"))
              .csv(featureUris.toList: _*)

            limit.foreach { n =>
              featuresDF = featuresDF.limit(n)
            }

            //          featuresDF.select("gid_0").distinct().show()

            /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
            val featureRDD: RDD[Feature[Geometry, TreeLossRowFeatureId]] =
              featuresDF.rdd.map { row: Row =>
                val cell_id: Int = row.getString(3).toInt
                val geom: Geometry = WKB.read(row.getString(1))
                Feature(geom, TreeLossRowFeatureId(cell_id))
              }

            val part = new HashPartitioner(
              partitions = featureRDD.getNumPartitions * inputPartitionMultiplier
            )

            val summaryRDD: RDD[(TreeLossRowFeatureId, TreeLossSummary)] =
              TreeLossRDD(featureRDD, TreeLossGrid.blockTileGrid, part)

            val summaryDF =
              summaryRDD
                .flatMap {
                  case (id, treeLossSummary) =>
                    treeLossSummary.stats.map {
                      case (lossDataGroup, lossData) => {

                        TreeLossRow(
                          TreeLossRowFeatureId(id.cell_id),
                          lossDataGroup.threshold,
                          lossData.extent2010,
                          lossData.totalArea,
                          TreeLossYearDataMap.toList(lossData.lossYear)
                        )
                      }
                    }
                }
                .toDF(
                  "feature_id",
                  "threshold",
                  "extent_2010",
                  "total_area",
                  "year_data"
                )

            val runOutputUrl = outputUrl + "/treecoverloss_" +
              DateTimeFormatter
                .ofPattern("yyyyMMdd_HHmm")
                .format(LocalDateTime.now)

            val dataframe = summaryDF
              .transform(DF.unpackValues)

            val csvOptions = Map(
              "header" -> "true",
              "delimiter" -> "\t",
              "quote" -> "\u0000",
              "quoteMode" -> "NONE",
              "nullValue" -> "\u0000"
            )

            dataframe
              .coalesce(1)
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl)

            spark.stop
        }
      }
    )
