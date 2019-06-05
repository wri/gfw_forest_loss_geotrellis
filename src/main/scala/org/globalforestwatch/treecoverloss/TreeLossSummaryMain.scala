package org.globalforestwatch.treecoverloss

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.monovore.decline.{CommandApp, Opts}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import cats.implicits._
import geotrellis.vector.io.wkb.WKB
import geotrellis.vector.{Feature, Geometry}
import org.globalforestwatch.features.GADMFeatureId
import org.locationtech.jts.precision.GeometryPrecisionReducer

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
          val spark: SparkSession = TreeLossSparkSession()

          import spark.implicits._

          // ref: https://github.com/databricks/spark-csv
          var featuresDF: DataFrame = spark.read
            .options(Map("header" -> "true", "delimiter" -> "\t"))
            .csv(featureUris.toList: _*)

          isoFirst.foreach { firstLetter =>
            featuresDF =
              featuresDF.filter(substring($"iso", 0, 1) === firstLetter(0))
          }

          isoStart.foreach { startCode =>
            featuresDF = featuresDF.filter($"iso" >= startCode)
          }

          isoEnd.foreach { endCode =>
            featuresDF = featuresDF.filter($"iso" < endCode)
          }

          iso.foreach { isoCode =>
            featuresDF = featuresDF.filter($"iso" === isoCode)
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
            featuresDF.rdd.mapPartitions({
              iter: Iterator[Row] =>
                // We need to reduce geometry precision  a bit to avoid issues like reported here
                // https://github.com/locationtech/geotrellis/issues/2951
                //
                // Precision is set in src/main/resources/application.conf
                // Here we use a fixed precision type and scale 1e8
                // This is more than enough given that we work with 30 meter pixels
                // and geometries already simplified to 1e11

                val gpr = new GeometryPrecisionReducer(
                  geotrellis.vector.GeomFactory.precisionModel
                )

                def reduce(
                            gpr: org.locationtech.jts.precision.GeometryPrecisionReducer
                          )(g: geotrellis.vector.Geometry): geotrellis.vector.Geometry =
                  geotrellis.vector.Geometry(gpr.reduce(g.jtsGeom))

                def isValidGeom(wkb: String): Boolean = {
                  val geom: Option[Geometry] = try {
                    Some(reduce(gpr)(WKB.read(wkb)))
                  } catch {
                    case ae: java.lang.AssertionError =>
                      println("There was an empty geometry")
                      None
                    case t: Throwable => throw t
                  }

                  geom match {
                    case Some(g) => true
                    case None => false
                  }
                }

                for {
                  i <- iter
                  if isValidGeom(i.getString(4))
                } yield {

                    val countryCode: String = i.getString(1)
                    val admin1: String = i.getString(2)
                    val admin2: String = i.getString(3)
                  val geom: Geometry = reduce(gpr)(WKB.read(i.getString(4)))
                    Feature(geom, GADMFeatureId(countryCode, admin1, admin2))
                  }
            }, preservesPartitioning = true)

          val part = new HashPartitioner(
            partitions = featureRDD.getNumPartitions * inputPartitionMultiplier
          )

          val summaryRDD: RDD[(GADMFeatureId, TreeLossSummary)] =
            TreeLossRDD(featureRDD, TreeLossGrid.blockTileGrid, part)

          val summaryDF =
            summaryRDD
              .flatMap {
                case (id, treeLossSummary) =>
                  treeLossSummary.stats.map {
                    case (lossDataGroup, lossData) => {

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

                      TreeLossRow(
                        TreeLossRowFeatureId(id.country, admin1, admin2),
                        lossDataGroup.threshold,
                        TreeLossRowLayers(
                          lossDataGroup.drivers,
                          lossDataGroup.globalLandCover,
                          lossDataGroup.primaryForest,
                          lossDataGroup.idnPrimaryForest,
                          lossDataGroup.erosion,
                          lossDataGroup.biodiversitySignificance,
                          lossDataGroup.biodiversityIntactness,
                          lossDataGroup.wdpa,
                          lossDataGroup.aze,
                          lossDataGroup.plantations,
                          lossDataGroup.riverBasins,
                          lossDataGroup.ecozones,
                          lossDataGroup.urbanWatersheds,
                          lossDataGroup.mangroves1996,
                          lossDataGroup.mangroves2016,
                          lossDataGroup.waterStress,
                          lossDataGroup.intactForestLandscapes,
                          lossDataGroup.endemicBirdAreas,
                          lossDataGroup.tigerLandscapes,
                          lossDataGroup.landmark,
                          lossDataGroup.landRights,
                          lossDataGroup.keyBiodiversityAreas,
                          lossDataGroup.mining,
                          lossDataGroup.rspo,
                          lossDataGroup.peatlands,
                          lossDataGroup.oilPalm,
                          lossDataGroup.idnForestMoratorium,
                          lossDataGroup.idnLandCover,
                          lossDataGroup.mexProtectedAreas,
                          lossDataGroup.mexPaymentForEcosystemServices,
                          lossDataGroup.mexForestZoning,
                          lossDataGroup.perProductionForest,
                          lossDataGroup.perProtectedAreas,
                          lossDataGroup.perForestConcessions,
                          lossDataGroup.braBiomes,
                          lossDataGroup.woodFiber,
                          lossDataGroup.resourceRights,
                          lossDataGroup.logging,
                          lossDataGroup.oilGas
                        ),
                        lossData.extent2000,
                        lossData.extent2010,
                        lossData.totalArea,
                        lossData.totalGainArea,
                        lossData.totalBiomass,
                        lossData.totalCo2,
                        lossData.biomassHistogram.mean(),
                        lossData.totalMangroveBiomass,
                        lossData.totalMangroveCo2,
                        lossData.mangroveBiomassHistogram.mean(),
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
                "total_mangrove_biomass",
                "total_mangrove_co2",
                "avg_mangrove_biomass_per_ha",
                "year_data"
              )

          val runOutputUrl = outputUrl + "/treecoverloss_" +
            DateTimeFormatter
              .ofPattern("yyyyMMdd_HHmm")
              .format(LocalDateTime.now)

          val outputPartitionCount =
            maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

          summaryDF.repartition($"feature_id", $"threshold")

          val adm2DF = summaryDF
            .transform(Adm2DF.unpackValues)

          //          adm2DF.repartition($"iso")
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
          adm2DF.unpersist()

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
