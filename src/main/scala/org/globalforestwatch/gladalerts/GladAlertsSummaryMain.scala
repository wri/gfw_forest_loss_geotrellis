package org.globalforestwatch.gladalerts

import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.globalforestwatch.features.{GadmFeature, GadmFeatureFilter, GadmFeatureId}


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

        val tclOpt = Opts.flag("tcl", "TCL tile extent").orFalse

        val gladOpt = Opts.flag("glad", "GLAD tile extent").orFalse

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
          admin2Opt,
          tclOpt,
          gladOpt
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
           admin2,
           tcl,
           glad) =>
            val spark: SparkSession = GladAlertsSparkSession()

            import spark.implicits._

            // ref: https://github.com/databricks/spark-csv
            val featuresDF: DataFrame = spark.read
              .options(Map("header" -> "true", "delimiter" -> "\t"))
              .csv(featureUris.toList: _*)
              .transform(
                GadmFeatureFilter.filter(
                  isoFirst,
                  isoStart,
                  isoEnd,
                  iso,
                  admin1,
                  admin2,
                  limit,
                  tcl,
                  glad
                )
              )

            /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
            val featureRDD: RDD[Feature[Geometry, GadmFeatureId]] =
              featuresDF.rdd.mapPartitions({ iter: Iterator[Row] =>
                for {
                  i <- iter
                  if GadmFeature.isValidGeom(i)
                } yield {
                  GadmFeature.getFeature(i)
                }
              }, preservesPartitioning = true)

            val part = new HashPartitioner(
              partitions = featureRDD.getNumPartitions * inputPartitionMultiplier
            )

            val summaryRDD: RDD[(GadmFeatureId, GladAlertsSummary)] =
              GladAlertsRDD(featureRDD, GladAlertsGrid.blockTileGrid, part)

            val summaryDF =
              summaryRDD
                .flatMap {
                  case (id, gladAlertSummary) =>
                    gladAlertSummary.stats.map {
                      case (gladAlertsDataGroup, gladAlertsData) => {

                        val admin1: Integer = id.adm1ToInt
                        val admin2: Integer = id.adm2ToInt

                        val alertDate: String = {
                          gladAlertsDataGroup.alertDate match {
                            case Some(d: LocalDate) => d.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                            case _ => null
                          }
                        }

                        GladAlertsRow(
                          id.country,
                          admin1,
                          admin2,
                          alertDate,
                          gladAlertsDataGroup.isConfirmed,
                          gladAlertsDataGroup.tile.x,
                          gladAlertsDataGroup.tile.y,
                          gladAlertsDataGroup.tile.z,
                          GladAlertsRowLayers(
                            gladAlertsDataGroup.climateMask,
                            gladAlertsDataGroup.primaryForest,
                            gladAlertsDataGroup.protectedAreas,
                            gladAlertsDataGroup.aze,
                            gladAlertsDataGroup.keyBiodiversityAreas,
                            gladAlertsDataGroup.landmark,
                            gladAlertsDataGroup.plantations,
                            gladAlertsDataGroup.mining,
                            gladAlertsDataGroup.logging,
                            gladAlertsDataGroup.rspo,
                            gladAlertsDataGroup.woodFiber,
                            gladAlertsDataGroup.peatlands,
                            gladAlertsDataGroup.indonesiaForestMoratorium,
                            gladAlertsDataGroup.oilPalm,
                            gladAlertsDataGroup.indonesiaForestArea,
                            gladAlertsDataGroup.peruForestConcessions,
                            gladAlertsDataGroup.oilGas,
                            gladAlertsDataGroup.mangroves2016,
                            gladAlertsDataGroup.intactForestLandscapes2016,
                            gladAlertsDataGroup.braBiomes
                          ),
                          gladAlertsData.totalAlerts,
                          gladAlertsData.alertArea,
                          gladAlertsData.co2Emissions,
                          gladAlertsData.totalArea
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
                  "layers",
                  "alert_count",
                  "alert_area_ha",
                  "co2_emissions_Mt",
                  "total_area_ha"
                )

            val csvOptions = Map(
              "header" -> "true",
              "delimiter" -> ",",
              "quote" -> "\u0000",
              "quoteMode" -> "NONE",
              "nullValue" -> "\u0000"
            )

            val runOutputUrl = outputUrl +
              "/gladAlerts_" + DateTimeFormatter
              .ofPattern("yyyyMMdd_HHmm")
              .format(LocalDateTime.now)

            val outputPartitionCount =
              maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

            summaryDF.repartition(partitionExprs = $"iso", $"adm1", $"adm2")

            summaryDF.cache()

            val tileDF = summaryDF
              .transform(TileDF.sumAlerts)

            tileDF
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/tiles")

            val adm2DailyDF = summaryDF
              .transform(Adm2DailyDF.unpackValues)
              .transform(Adm2DailyDF.sumAlerts)

            summaryDF.unpersist()

            adm2DailyDF
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/adm2_daily")

            val adm2WeeklyDF = adm2DailyDF.transform(Adm2WeeklyDF.sumAlerts)

            adm2WeeklyDF
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/adm2_weekly")

            val adm1WeeklyDF = adm2WeeklyDF
              .transform(Adm1WeeklyDF.sumAlerts)

            adm1WeeklyDF
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/adm1_weekly")

            val isoWeeklyDF = adm1WeeklyDF
              .transform(IsoWeeklyDF.sumAlerts)

            isoWeeklyDF
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/iso_weekly")

            spark.stop
        }
      }
    )
