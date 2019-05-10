package org.globalforestwatch.gladalerts

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
import org.apache.spark.sql.functions._
import org.globalforestwatch.features.GADMFeatureId

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

        val logger = Logger.getLogger("TreeLossSummaryMain")

        (
          featuresOpt,
          outputOpt,
          intputPartitionsOpt,
          outputPartitionsOpt,
          limitOpt,
          isoOpt,
          isoFirstOpt,
          isoStartOpt,
          isoEndOpt,
          admin1Opt,
          admin2Opt
        ).mapN {
          (featureUris,
           outputUrl,
           inputPartitionMultiplier,
           maybeOutputPartitions,
           limit,
           iso,
           isoFirst,
           isoStart,
           isoEnd,
           admin1,
           admin2) =>
            val spark: SparkSession = GladAlertsSparkSession.spark

            import spark.implicits._

            // ref: https://github.com/databricks/spark-csv
            var featuresDF: DataFrame = spark.read
              .options(Map("header" -> "true", "delimiter" -> "\t"))
              .csv(featureUris.toList: _*)

            isoFirst.foreach { firstLetter =>
              featuresDF =
                featuresDF.filter(substring($"gid_0", 0, 1) === firstLetter(0))
            }

            isoStart.foreach { startCode =>
              featuresDF = featuresDF.filter($"gid_0" >= startCode)
            }

            isoEnd.foreach { endCode =>
              featuresDF = featuresDF.filter($"gid_0" < endCode)
            }

            iso.foreach { isoCode =>
              featuresDF = featuresDF.filter($"gid_0" === isoCode)
            }

            admin1.foreach { admin1Code =>
              featuresDF = featuresDF.filter($"gid_1" === admin1Code)
            }

            admin2.foreach { admin2Code =>
              featuresDF = featuresDF.filter($"gid_2" === admin2Code)
            }

            limit.foreach { n =>
              featuresDF = featuresDF.limit(n)
            }

            //          featuresDF.select("gid_0").distinct().show()

            /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
            val featureRDD: RDD[Feature[Geometry, GADMFeatureId]] =
              featuresDF.rdd.map { row: Row =>
                val countryCode: String = row.getString(2)
                val admin1: String = row.getString(3)
                val admin2: String = row.getString(4)
                val geom: Geometry = WKB.read(row.getString(5))
                Feature(geom, GADMFeatureId(countryCode, admin1, admin2))
              }

            val part = new HashPartitioner(
              partitions = featureRDD.getNumPartitions * inputPartitionMultiplier
            )

            val summaryRDD: RDD[(GADMFeatureId, GladAlertsSummary)] =
              GladAlertsRDD(featureRDD, GladAlertsGrid.blockTileGrid, part)

            val summaryDF =
              summaryRDD
                .flatMap {
                  case (id, gladAlertsSummary) =>
                    gladAlertsSummary.stats.map {
                      case (gladAlertsDataGroup, gladAlertsData) => {

                        val admin1: Integer = try {
                          id.admin1.split("[.]")(1).split("[_]")(0).toInt
                        } catch {
                          case e: Exception => null
                        }

                        val admin2: Integer = try {
                          id.admin2.split("[.]")(2).split("[_]")(0).toInt
                        } catch {
                          case e: Exception => null
                        }

                        GladAlertsRow(
                          id.country,
                          admin1,
                          admin2,
                          gladAlertsDataGroup.alertDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                          gladAlertsDataGroup.isConfirmed,
                          gladAlertsDataGroup.tile.x,
                          gladAlertsDataGroup.tile.y,
                          gladAlertsDataGroup.tile.z,
                          gladAlertsDataGroup.climateMask,
                          gladAlertsData.totalAlerts,
                          gladAlertsData.totalArea,
                          gladAlertsData.totalCo2

                        )
                      }
                    }
                }
                .toDF(
                  "iso",
                  "adm1",
                  "adm2",
                  "alert_date",
                  "is_confirmed",
                  "x",
                  "y",
                  "z",
                  "climate_mask",
                  "alerts",
                  "area",
                  "co2"
                )

            val runOutputUrl = outputUrl +
              "/gladAlerts_" + DateTimeFormatter
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

            summaryDF
              .coalesce(1)
              //              .orderBy($"iso", $"threshold")
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/zoom12")

            spark.stop
        }
      }
    )
