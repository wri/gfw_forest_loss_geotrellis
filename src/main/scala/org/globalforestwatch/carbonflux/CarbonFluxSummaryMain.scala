package org.globalforestwatch.carbonflux

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

object CarbonFluxSummaryMain
    extends CommandApp(
      name = "geotrellis-carbon-flux-summary",
      header = "Compute statistics on carbon flux",
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

        val logger = Logger.getLogger("CarbonFluxSummaryMain")

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
            val spark: SparkSession = CarbonFluxSparkSession()

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
                      case None    => false
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

            val summaryRDD: RDD[(GADMFeatureId, CarbonFluxSummary)] =
              CarbonFluxRDD(featureRDD, CarbonFluxGrid.blockTileGrid, part)

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

                        CarbonFluxRow(
                          CarbonFluxRowFeatureId(id.country, admin1, admin2),
                          lossDataGroup.threshold,
                          CarbonFluxRowLayers(
                            lossDataGroup.gain,
                            lossDataGroup.mangroveBiomassExtent,
                            lossDataGroup.drivers,
                            lossDataGroup.ecozones,
                            lossDataGroup.landRights,
                            lossDataGroup.wdpa,
                            lossDataGroup.intactForestLandscapes,
                            lossDataGroup.plantations
                          ),
                          lossData.extent2000,
                          lossData.totalArea,
                          lossData.totalBiomass,
                          lossData.biomassHistogram.mean(),
                          lossData.totalGrossAnnualRemovalsCarbon,
                          lossData.grossAnnualRemovalsCarbonHistogram.mean(),
                          lossData.totalGrossCumulRemovalsCarbon,
                          lossData.grossCumulRemovalsCarbonHistogram.mean(),
                          lossData.totalNetFluxCo2,
                          lossData.netFluxCo2Histogram.mean(),
                          lossData.totalAgcEmisYear,
                          lossData.agcEmisYearHistogram.mean(),
                          lossData.totalBgcEmisYear,
                          lossData.bgcEmisYearHistogram.mean(),
                          lossData.totalDeadwoodCarbonEmisYear,
                          lossData.deadwoodCarbonEmisYearHistogram.mean(),
                          lossData.totalLitterCarbonEmisYear,
                          lossData.litterCarbonEmisYearHistogram.mean(),
                          lossData.totalSoilCarbonEmisYear,
                          lossData.soilCarbonEmisYearHistogram.mean(),
                          lossData.totalCarbonEmisYear,
                          lossData.totalCarbonEmisYearHistogram.mean(),
                          lossData.totalAgc2000,
                          lossData.agc2000Histogram.mean(),
                          lossData.totalBgc2000,
                          lossData.bgc2000Histogram.mean(),
                          lossData.totalDeadwoodCarbon2000,
                          lossData.deadwoodCarbon2000Histogram.mean(),
                          lossData.totalLitterCarbon2000,
                          lossData.litterCarbon2000Histogram.mean(),
                          lossData.totalSoil2000Year,
                          lossData.soilCarbon2000Histogram.mean(),
                          lossData.totalCarbon2000,
                          lossData.totalCarbon2000Histogram.mean(),
                          lossData.totalGrossEmissionsCo2,
                          lossData.grossEmissionsCo2Histogram.mean(),
                          CarbonFluxYearDataMap.toList(lossData.lossYear)
                        )
                      }
                    }
                }
                .toDF(
                  "feature_id",
                  "threshold",
                  "layers",
                  "extent_2000",
                  "total_area",
                  "total_biomass",
                  "avg_biomass_per_ha",
                  "gross_annual_removals_carbon",
                  "avg_gross_annual_removals_carbon_ha",
                  "gross_cumul_removals_carbon",
                  "avg_gross_cumul_removals_carbon_ha",
                  "net_flux_co2",
                  "avg_net_flux_co2_ha",
                  "agc_emissions_year",
                  "avg_agc_emissions_year",
                  "bgc_emissions_year",
                  "avg_bgc_emissions_year",
                  "deadwood_carbon_emissions_year",
                  "avg_deadwood_carbon_emissions_year",
                  "litter_carbon_emissions_year",
                  "avg_litter_carbon_emissions_year",
                  "soil_carbon_emissions_year",
                  "avg_soil_carbon_emissions_year",
                  "total_carbon_emissions_year",
                  "avg_carbon_emissions_year",
                  "agc_2000",
                  "avg_agc_2000",
                  "bgc_2000",
                  "avg_bgc_2000",
                  "deadwood_carbon_2000",
                  "avg_deadwood_carbon_2000",
                  "litter_carbon_2000",
                  "avg_litter_carbon_2000",
                  "soil_2000_year",
                  "avg_soil_carbon_2000",
                  "total_carbon_2000",
                  "avg_carbon_2000",
                  "gross_emissions_co2",
                  "avg_gross_emissions_co2",
                  "year_data"
                )

            val runOutputUrl = outputUrl + "/carbonflux_" +
              DateTimeFormatter
                .ofPattern("yyyyMMdd_HHmm")
                .format(LocalDateTime.now)

            val outputPartitionCount =
              maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

            summaryDF.repartition($"feature_id", $"threshold")

            val csvOptions = Map(
              "header" -> "true",
              "delimiter" -> "\t",
              "quote" -> "\u0000",
              "quoteMode" -> "NONE",
              "nullValue" -> "\u0000"
            )

            val apiDF = summaryDF
              .transform(ApiDF.unpackValues)
              //              //              .transform(ApiDF.setNull)
              //              .coalesce(50)
              .orderBy(
                $"iso",
                $"adm1",
                $"adm2",
                $"threshold"
              )
              .write
              .options(csvOptions)
              .csv(path = runOutputUrl + "/summary/adm2")


            spark.stop
        }
      }
    )
