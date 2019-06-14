package org.globalforestwatch.treecoverloss

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

      val thresholdOpt = Opts
        .options[Int]("threshold", "Treecover threshold to apply")
        .withDefault(List(30))

      val primartyForestOpt = Opts
        .flag("primary-forests", "Include Primary Forests")
        .orFalse
        .withDefault(false)

      val limitOpt = Opts
        .option[Int]("limit", help = "Limit number of records processed")
        .orNone

      val idStartOpt =
        Opts
          .option[String](
          "id_start",
          help = "Filter by Feature IDs larger than or equal to given value"
        )
          .orNone

      val idEndOpt =
        Opts
          .option[String](
          "id_end",
          help = "Filter by Feature IDs smaller than given value"
        )
          .orNone

      val logger = Logger.getLogger("TreeLossSummaryMain")

      (
        featuresOpt,
        outputOpt,
        intputPartitionsOpt,
        outputPartitionsOpt,
        thresholdOpt,
        primartyForestOpt,
        limitOpt,
        idStartOpt,
        idEndOpt
      ).mapN {
        (featureUris,
         outputUrl,
         inputPartitionMultiplier,
         maybeOutputPartitions,
         thresholdFilter,
         includePrimaryForest,
         limit,
         idStart,
         idEnd) =>
          val spark: SparkSession = TreeLossSparkSession()

          import spark.implicits._

          // ref: https://github.com/databricks/spark-csv
          var featuresDF: DataFrame = spark.read
            .options(Map("header" -> "true", "delimiter" -> "\t"))
            .csv(featureUris.toList: _*)

          idStart.foreach { startID =>
            featuresDF = featuresDF.filter($"fid" >= startID)
          }

          idEnd.foreach { endID =>
            featuresDF = featuresDF.filter($"fid" < endID)
          }

          limit.foreach { n =>
            featuresDF = featuresDF.limit(n)
          }

          //          featuresDF.select("gid_0").distinct().show()

          /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
          val featureRDD: RDD[Feature[Geometry, Int]] =
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
                  if isValidGeom(i.getString(1))
                } yield {

                  val featureid: Int = i.getInt(0)
                  val geom: Geometry = reduce(gpr)(WKB.read(i.getString(1)))
                  Feature(geom, featureid)
                  }
            }, preservesPartitioning = true)

          val part = new HashPartitioner(
            partitions = featureRDD.getNumPartitions * inputPartitionMultiplier
          )

          val summaryRDD: RDD[(Int, TreeLossSummary)] =
            TreeLossRDD(featureRDD, TreeLossGrid.blockTileGrid, part)

          val summaryDF =
            summaryRDD
              .flatMap {
                case (id, treeLossSummary) =>
                  treeLossSummary.stats.map {
                    case (lossDataGroup, lossData) => {

                      TreeLossRow(
                        id,
                        lossDataGroup.threshold,
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

          summaryDF
            .filter($"threshold" isin thresholdFilter)
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
