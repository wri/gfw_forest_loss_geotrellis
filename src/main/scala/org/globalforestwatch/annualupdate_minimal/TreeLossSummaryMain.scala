package org.globalforestwatch.annualupdate_minimal

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.globalforestwatch.features.{
  GadmFeature,
  GadmFeatureFilter,
  GadmFeatureId
}

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
            val spark: SparkSession = TreeLossSparkSession()

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

            val summaryRDD: RDD[(GadmFeatureId, TreeLossSummary)] =
              TreeLossRDD(featureRDD, TreeLossGrid.blockTileGrid, part)

            val summaryDF =
              summaryRDD
                .flatMap {
                  case (id, treeLossSummary) =>
                    treeLossSummary.stats.map {
                      case (lossDataGroup, lossData) => {

                        val admin1: Integer = id.adm1ToInt
                        val admin2: Integer = id.adm2ToInt

                        TreeLossRow(
                          TreeLossRowFeatureId(id.country, admin1, admin2),
                          lossDataGroup.threshold,
                          TreeLossRowLayers(
                            lossDataGroup.drivers,
                            lossDataGroup.globalLandCover,
                            lossDataGroup.primaryForest,
//                            lossDataGroup.idnPrimaryForest,
//                            lossDataGroup.erosion,
//                            lossDataGroup.biodiversitySignificance,
//                            lossDataGroup.biodiversityIntactness,
                            lossDataGroup.wdpa,
                            lossDataGroup.aze,
                            lossDataGroup.plantations,
//                            lossDataGroup.riverBasins,
//                            lossDataGroup.ecozones,
//                            lossDataGroup.urbanWatersheds,
//                            lossDataGroup.mangroves1996,
//                            lossDataGroup.mangroves2016,
//                            lossDataGroup.waterStress,
                            lossDataGroup.intactForestLandscapes,
//                            lossDataGroup.endemicBirdAreas,
                            lossDataGroup.tigerLandscapes,
                            lossDataGroup.landmark,
                            lossDataGroup.landRights,
                            lossDataGroup.keyBiodiversityAreas,
                            lossDataGroup.mining,
//                            lossDataGroup.rspo,
//                            lossDataGroup.peatlands,
                            lossDataGroup.oilPalm,
                            lossDataGroup.idnForestMoratorium,
//                            lossDataGroup.idnLandCover,
//                            lossDataGroup.mexProtectedAreas,
//                            lossDataGroup.mexPaymentForEcosystemServices,
//                            lossDataGroup.mexForestZoning,
//                            lossDataGroup.perProductionForest,
//                            lossDataGroup.perProtectedAreas,
//                            lossDataGroup.perForestConcessions,
//                            lossDataGroup.braBiomes,
                            lossDataGroup.woodFiber,
                            lossDataGroup.resourceRights,
                            lossDataGroup.logging
//                            lossDataGroup.oilGas
                          ),
                          lossData.extent2000,
                          lossData.extent2010,
                          lossData.totalArea,
                          lossData.totalGainArea,
                          lossData.totalBiomass,
                          lossData.totalCo2,
                          lossData.biomassHistogram.mean(),
//                          lossData.totalMangroveBiomass,
//                          lossData.totalMangroveCo2,
//                          lossData.mangroveBiomassHistogram.mean(),
                          TreeLossYearDataMap.toList(lossData.lossYear)
                        )
                      }
                    }
                }
                .toDF(
                  "feature_id",
                  "threshold",
                  "layers",
                  "extent_2000",
                  "extent_2010",
                  "total_area",
                  "total_gain",
                  "total_biomass",
                  "total_co2",
                  "avg_biomass_per_ha",
//                  "total_mangrove_biomass",
//                  "total_mangrove_co2",
//                  "avg_mangrove_biomass_per_ha",
                  "year_data"
                )

            val runOutputUrl = outputUrl + "/annualupdate_minimal_" +
              DateTimeFormatter
                .ofPattern("yyyyMMdd_HHmm")
                .format(LocalDateTime.now)

            val outputPartitionCount =
              maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

            summaryDF.repartition($"feature_id", $"threshold")

            val adm2DF = summaryDF
              .transform(Adm2DF.unpackValues)

            adm2DF.repartition($"iso")
            adm2DF.cache()

            val csvOptions = Map(
              "header" -> "true",
              "delimiter" -> "\t",
              "quote" -> "\u0000",
              "quoteMode" -> "NONE",
              "nullValue" -> "\u0000"
            )

            val adm2SummaryDF = adm2DF
              .transform(Adm2SummaryDF.sumArea)

            adm2SummaryDF
              .transform(Adm2SummaryDF.roundValues)
              .coalesce(1)
              .orderBy(
                $"country",
                $"subnational1",
                $"subnational2",
                $"threshold"
              )
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/summary/adm2")

            val adm1SummaryDF = adm2SummaryDF.transform(Adm1SummaryDF.sumArea)

            adm1SummaryDF
              .transform(Adm1SummaryDF.roundValues)
              .coalesce(1)
              .orderBy($"country", $"subnational1", $"threshold")
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/summary/adm1")

            val isoSummaryDF = adm1SummaryDF.transform(IsoSummaryDF.sumArea)

            isoSummaryDF
              .transform(IsoSummaryDF.roundValues)
              .coalesce(1)
              .orderBy($"iso", $"threshold")
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/summary/iso")

            val apiDF = adm2DF
              .transform(ApiDF.setNull)

            apiDF.cache()
//            adm2DF.unpersist()

            val adm2ApiDF = apiDF
              .transform(Adm2ApiDF.nestYearData)

            adm2ApiDF
            //.coalesce(1)
            //            .repartition(
            //            outputPartitionCount,
            //            $"iso",
            //            $"adm1",
            //            $"adm2",
            //            $"threshold"
            //          )
            //            .orderBy($"iso", $"adm1", $"adm2", $"threshold")
            .toJSON
              .mapPartitions(vals => Iterator("[" + vals.mkString(",") + "]"))
              .write
              .text(runOutputUrl + "/api/adm2")

            val tempApiDF = apiDF
              .transform(Adm1ApiDF.sumArea)

            tempApiDF.cache()
            apiDF.unpersist()

            val adm1ApiDF = tempApiDF
              .transform(Adm1ApiDF.nestYearData)

            adm1ApiDF
            //            .repartition(outputPartitionCount, $"iso", $"adm1", $"threshold")
            //            .orderBy($"iso", $"adm1", $"threshold")
            .toJSON
              .mapPartitions(vals => Iterator("[" + vals.mkString(",") + "]"))
              .write
              .text(runOutputUrl + "/api/adm1")

            val isoApiDF = tempApiDF
              .transform(IsoApiDF.sumArea)
              .transform(IsoApiDF.nestYearData)

            isoApiDF
            //            .repartition(outputPartitionCount, $"iso", $"threshold")
            //            .orderBy($"iso", $"threshold")
            .toJSON
              .mapPartitions(vals => Iterator("[" + vals.mkString(",") + "]"))
              .write
              .text(runOutputUrl + "/api/iso")

            tempApiDF.unpersist()

            spark.stop
        }
      }
    )
