package org.globalforestwatch.treecoverloss

import com.monovore.decline.{CommandApp, Opts}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import cats.implicits._
import geotrellis.vector.io.wkb.WKB
import geotrellis.vector.{Feature, Geometry}

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
        admin1Opt,
        admin2Opt
      ).mapN {
        (featureUris,
         outputUrl,
         inputPartitionMultiplier,
         maybeOutputPartitions,
         limit,
         iso,
         admin1,
         admin2) =>
          val conf = new SparkConf()
            .setIfMissing("spark.master", "local[*]")
            .setAppName("Tree Cover Loss DataFrame")
            .set(
              "spark.serializer",
              "org.apache.spark.serializer.KryoSerializer"
            )
            .set(
              "spark.kryo.registrator",
              "geotrellis.spark.io.kryo.KryoRegistrator"
            )
            .set("spark.sql.crossJoin.enabled", "true")

          implicit val spark: SparkSession =
            SparkSession.builder.config(conf).getOrCreate
          import spark.implicits._

          // ref: https://github.com/databricks/spark-csv
          var featuresDF: DataFrame = spark.read
            .options(Map("header" -> "true", "delimiter" -> "\t"))
            .csv(featureUris.toList: _*)

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

          /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
          val featureRDD: RDD[Feature[Geometry, FeatureId]] =
            featuresDF.rdd.map { row: Row =>
              val countryCode: String = row.getString(2)
              val admin1: String = row.getString(3)
              val admin2: String = row.getString(4)
              val geom: Geometry = WKB.read(row.getString(5))
              Feature(geom, FeatureId(countryCode, admin1, admin2))
            }

          val part = new HashPartitioner(
            partitions = featureRDD.getNumPartitions * inputPartitionMultiplier
          )

          val summaryRDD: RDD[(FeatureId, TreeLossSummary)] =
            TreeLossRDD(featureRDD, TenByTenGrid.blockTileGrid, part)

          val summaryDF =
            summaryRDD
              .flatMap {
                case (id, treeLossSummary) =>
                  treeLossSummary.stats.map {
                    case (lossDataGroup, lossData) => {

                      val admin1 =
                        if (id.admin1.length > 0)
                          id.admin1.split("[.]")(1).split("[_]")(0)
                        else ""
                      val admin2 =
                        if (id.admin2.length > 0)
                          id.admin2.split("[.]")(2).split("[_]")(0)
                        else ""

                      LossRow(
                        LossRowFeatureId(id.country, admin1, admin2),
                        lossDataGroup.tcd2000,
                        lossDataGroup.tcd2010,
                        LossRowLayers(
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
                        lossData.totalArea,
                        lossData.totalGainArea,
                        lossData.totalBiomass,
                        lossData.totalCo2,
                        lossData.biomassHistogram.mean(),
                        lossData.totalMangroveBiomass,
                        lossData.totalMangroveCo2,
                        lossData.mangroveBiomassHistogram.mean(),
                        LossYearDataMap.toList(lossData.lossYear)
                      )
                    }
                  }
              }
              .toDF(
                "feature_id",
                "threshold_2000",
                "threshold_2010",
                "layers",
                "area",
                "gain",
                "biomass",
                "co2",
                "biomass_per_ha",
                "mangrove_biomass",
                "mangrove_co2",
                "mangrove_biomass_per_ha",
                "year_data"
              )

          summaryDF.cache()

          val WindowPartitionOrder = Window
            .partitionBy($"m_feature_id", $"m_layers")
            .orderBy($"m_threshold".desc)

          def windowSum(col: String) = sum(col).over(WindowPartitionOrder)

          val thresholdDF =
            Seq(0, 10, 15, 20, 25, 30, 50, 75).toDF("threshold")

          val tAdm2DF = summaryDF
            .groupBy("feature_id", "layers")
            .agg(sum("area") as "totalarea")
            .crossJoin(thresholdDF)
            .select(
              $"feature_id" as "m_feature_id",
              $"layers" as "m_layers",
              $"threshold" as "m_threshold",
              $"totalarea"
            )

          tAdm2DF.cache()

          val sAnnualLossDF = summaryDF
            .select(
              $"feature_id",
              $"threshold_2000" as "threshold",
              $"layers",
              $"area",
              $"biomass",
              $"co2",
              $"mangrove_biomass",
              $"mangrove_co2",
              'year_data.getItem(0).getItem("year") as "year_2001",
              'year_data.getItem(1).getItem("year") as "year_2002",
              'year_data.getItem(2).getItem("year") as "year_2003",
              'year_data.getItem(3).getItem("year") as "year_2004",
              'year_data.getItem(4).getItem("year") as "year_2005",
              'year_data.getItem(5).getItem("year") as "year_2006",
              'year_data.getItem(6).getItem("year") as "year_2007",
              'year_data.getItem(7).getItem("year") as "year_2008",
              'year_data.getItem(8).getItem("year") as "year_2009",
              'year_data.getItem(9).getItem("year") as "year_2010",
              'year_data.getItem(10).getItem("year") as "year_2011",
              'year_data.getItem(11).getItem("year") as "year_2012",
              'year_data.getItem(12).getItem("year") as "year_2013",
              'year_data.getItem(13).getItem("year") as "year_2014",
              'year_data.getItem(14).getItem("year") as "year_2015",
              'year_data.getItem(15).getItem("year") as "year_2016",
              'year_data.getItem(16).getItem("year") as "year_2017",
              'year_data.getItem(17).getItem("year") as "year_2018",
              'year_data.getItem(0).getItem("area_loss") as "area_loss_2001",
              'year_data.getItem(1).getItem("area_loss") as "area_loss_2002",
              'year_data.getItem(2).getItem("area_loss") as "area_loss_2003",
              'year_data.getItem(3).getItem("area_loss") as "area_loss_2004",
              'year_data.getItem(4).getItem("area_loss") as "area_loss_2005",
              'year_data.getItem(5).getItem("area_loss") as "area_loss_2006",
              'year_data.getItem(6).getItem("area_loss") as "area_loss_2007",
              'year_data.getItem(7).getItem("area_loss") as "area_loss_2008",
              'year_data.getItem(8).getItem("area_loss") as "area_loss_2009",
              'year_data.getItem(9).getItem("area_loss") as "area_loss_2010",
              'year_data.getItem(10).getItem("area_loss") as "area_loss_2011",
              'year_data.getItem(11).getItem("area_loss") as "area_loss_2012",
              'year_data.getItem(12).getItem("area_loss") as "area_loss_2013",
              'year_data.getItem(13).getItem("area_loss") as "area_loss_2014",
              'year_data.getItem(14).getItem("area_loss") as "area_loss_2015",
              'year_data.getItem(15).getItem("area_loss") as "area_loss_2016",
              'year_data.getItem(16).getItem("area_loss") as "area_loss_2017",
              'year_data.getItem(17).getItem("area_loss") as "area_loss_2018",
              'year_data
                .getItem(0)
                .getItem("biomass_loss") as "biomass_loss_2001",
              'year_data
                .getItem(1)
                .getItem("biomass_loss") as "biomass_loss_2002",
              'year_data
                .getItem(2)
                .getItem("biomass_loss") as "biomass_loss_2003",
              'year_data
                .getItem(3)
                .getItem("biomass_loss") as "biomass_loss_2004",
              'year_data
                .getItem(4)
                .getItem("biomass_loss") as "biomass_loss_2005",
              'year_data
                .getItem(5)
                .getItem("biomass_loss") as "biomass_loss_2006",
              'year_data
                .getItem(6)
                .getItem("biomass_loss") as "biomass_loss_2007",
              'year_data
                .getItem(7)
                .getItem("biomass_loss") as "biomass_loss_2008",
              'year_data
                .getItem(8)
                .getItem("biomass_loss") as "biomass_loss_2009",
              'year_data
                .getItem(9)
                .getItem("biomass_loss") as "biomass_loss_2010",
              'year_data
                .getItem(10)
                .getItem("biomass_loss") as "biomass_loss_2011",
              'year_data
                .getItem(11)
                .getItem("biomass_loss") as "biomass_loss_2012",
              'year_data
                .getItem(12)
                .getItem("biomass_loss") as "biomass_loss_2013",
              'year_data
                .getItem(13)
                .getItem("biomass_loss") as "biomass_loss_2014",
              'year_data
                .getItem(14)
                .getItem("biomass_loss") as "biomass_loss_2015",
              'year_data
                .getItem(15)
                .getItem("biomass_loss") as "biomass_loss_2016",
              'year_data
                .getItem(16)
                .getItem("biomass_loss") as "biomass_loss_2017",
              'year_data
                .getItem(17)
                .getItem("biomass_loss") as "biomass_loss_2018",
              'year_data
                .getItem(0)
                .getItem("carbon_emissions") as "carbon_emissions_2001",
              'year_data
                .getItem(1)
                .getItem("carbon_emissions") as "carbon_emissions_2002",
              'year_data
                .getItem(2)
                .getItem("carbon_emissions") as "carbon_emissions_2003",
              'year_data
                .getItem(3)
                .getItem("carbon_emissions") as "carbon_emissions_2004",
              'year_data
                .getItem(4)
                .getItem("carbon_emissions") as "carbon_emissions_2005",
              'year_data
                .getItem(5)
                .getItem("carbon_emissions") as "carbon_emissions_2006",
              'year_data
                .getItem(6)
                .getItem("carbon_emissions") as "carbon_emissions_2007",
              'year_data
                .getItem(7)
                .getItem("carbon_emissions") as "carbon_emissions_2008",
              'year_data
                .getItem(8)
                .getItem("carbon_emissions") as "carbon_emissions_2009",
              'year_data
                .getItem(9)
                .getItem("carbon_emissions") as "carbon_emissions_2010",
              'year_data
                .getItem(10)
                .getItem("carbon_emissions") as "carbon_emissions_2011",
              'year_data
                .getItem(11)
                .getItem("carbon_emissions") as "carbon_emissions_2012",
              'year_data
                .getItem(12)
                .getItem("carbon_emissions") as "carbon_emissions_2013",
              'year_data
                .getItem(13)
                .getItem("carbon_emissions") as "carbon_emissions_2014",
              'year_data
                .getItem(14)
                .getItem("carbon_emissions") as "carbon_emissions_2015",
              'year_data
                .getItem(15)
                .getItem("carbon_emissions") as "carbon_emissions_2016",
              'year_data
                .getItem(16)
                .getItem("carbon_emissions") as "carbon_emissions_2017",
              'year_data
                .getItem(17)
                .getItem("carbon_emissions") as "carbon_emissions_2018",
              'year_data
                .getItem(0)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2001",
              'year_data
                .getItem(1)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2002",
              'year_data
                .getItem(2)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2003",
              'year_data
                .getItem(3)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2004",
              'year_data
                .getItem(4)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2005",
              'year_data
                .getItem(5)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2006",
              'year_data
                .getItem(6)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2007",
              'year_data
                .getItem(7)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2008",
              'year_data
                .getItem(8)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2009",
              'year_data
                .getItem(9)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2010",
              'year_data
                .getItem(10)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2011",
              'year_data
                .getItem(11)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2012",
              'year_data
                .getItem(12)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2013",
              'year_data
                .getItem(13)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2014",
              'year_data
                .getItem(14)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2015",
              'year_data
                .getItem(15)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2016",
              'year_data
                .getItem(16)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2017",
              'year_data
                .getItem(17)
                .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2018",
              'year_data
                .getItem(0)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2001",
              'year_data
                .getItem(1)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2002",
              'year_data
                .getItem(2)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2003",
              'year_data
                .getItem(3)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2004",
              'year_data
                .getItem(4)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2005",
              'year_data
                .getItem(5)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2006",
              'year_data
                .getItem(6)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2007",
              'year_data
                .getItem(7)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2008",
              'year_data
                .getItem(8)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2009",
              'year_data
                .getItem(9)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2010",
              'year_data
                .getItem(10)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2011",
              'year_data
                .getItem(11)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2012",
              'year_data
                .getItem(12)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2013",
              'year_data
                .getItem(13)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2014",
              'year_data
                .getItem(14)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2015",
              'year_data
                .getItem(15)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2016",
              'year_data
                .getItem(16)
                .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2017",
              'year_data.getItem(17).getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2018"
            )
            .groupBy(
              $"feature_id",
              $"threshold",
              $"layers",
              $"year_2001",
              $"year_2002",
              $"year_2003",
              $"year_2004",
              $"year_2005",
              $"year_2006",
              $"year_2007",
              $"year_2008",
              $"year_2009",
              $"year_2010",
              $"year_2011",
              $"year_2012",
              $"year_2013",
              $"year_2014",
              $"year_2015",
              $"year_2016",
              $"year_2017",
              $"year_2018"
            )
            .agg(
              sum("area") as "area",
              sum("biomass") as "biomass",
              sum("co2") as "co2",
              sum("mangrove_biomass") as "mangrove_biomass",
              sum("mangrove_co2") as "mangrove_co2",
              sum("area_loss_2001") as "area_loss_2001",
              sum("area_loss_2002") as "area_loss_2002",
              sum("area_loss_2003") as "area_loss_2003",
              sum("area_loss_2004") as "area_loss_2004",
              sum("area_loss_2005") as "area_loss_2005",
              sum("area_loss_2006") as "area_loss_2006",
              sum("area_loss_2007") as "area_loss_2007",
              sum("area_loss_2008") as "area_loss_2008",
              sum("area_loss_2009") as "area_loss_2009",
              sum("area_loss_2010") as "area_loss_2010",
              sum("area_loss_2011") as "area_loss_2011",
              sum("area_loss_2012") as "area_loss_2012",
              sum("area_loss_2013") as "area_loss_2013",
              sum("area_loss_2014") as "area_loss_2014",
              sum("area_loss_2015") as "area_loss_2015",
              sum("area_loss_2016") as "area_loss_2016",
              sum("area_loss_2017") as "area_loss_2017",
              sum("area_loss_2018") as "area_loss_2018",
              sum("biomass_loss_2001") as "biomass_loss_2001",
              sum("biomass_loss_2002") as "biomass_loss_2002",
              sum("biomass_loss_2003") as "biomass_loss_2003",
              sum("biomass_loss_2004") as "biomass_loss_2004",
              sum("biomass_loss_2005") as "biomass_loss_2005",
              sum("biomass_loss_2006") as "biomass_loss_2006",
              sum("biomass_loss_2007") as "biomass_loss_2007",
              sum("biomass_loss_2008") as "biomass_loss_2008",
              sum("biomass_loss_2009") as "biomass_loss_2009",
              sum("biomass_loss_2010") as "biomass_loss_2010",
              sum("biomass_loss_2011") as "biomass_loss_2011",
              sum("biomass_loss_2012") as "biomass_loss_2012",
              sum("biomass_loss_2013") as "biomass_loss_2013",
              sum("biomass_loss_2014") as "biomass_loss_2014",
              sum("biomass_loss_2015") as "biomass_loss_2015",
              sum("biomass_loss_2016") as "biomass_loss_2016",
              sum("biomass_loss_2017") as "biomass_loss_2017",
              sum("biomass_loss_2018") as "biomass_loss_2018",
              sum("carbon_emissions_2001") as "carbon_emissions_2001",
              sum("carbon_emissions_2002") as "carbon_emissions_2002",
              sum("carbon_emissions_2003") as "carbon_emissions_2003",
              sum("carbon_emissions_2004") as "carbon_emissions_2004",
              sum("carbon_emissions_2005") as "carbon_emissions_2005",
              sum("carbon_emissions_2006") as "carbon_emissions_2006",
              sum("carbon_emissions_2007") as "carbon_emissions_2007",
              sum("carbon_emissions_2008") as "carbon_emissions_2008",
              sum("carbon_emissions_2009") as "carbon_emissions_2009",
              sum("carbon_emissions_2010") as "carbon_emissions_2010",
              sum("carbon_emissions_2011") as "carbon_emissions_2011",
              sum("carbon_emissions_2012") as "carbon_emissions_2012",
              sum("carbon_emissions_2013") as "carbon_emissions_2013",
              sum("carbon_emissions_2014") as "carbon_emissions_2014",
              sum("carbon_emissions_2015") as "carbon_emissions_2015",
              sum("carbon_emissions_2016") as "carbon_emissions_2016",
              sum("carbon_emissions_2017") as "carbon_emissions_2017",
              sum("carbon_emissions_2018") as "carbon_emissions_2018",
              sum("mangrove_biomass_loss_2001") as "mangrove_biomass_loss_2001",
              sum("mangrove_biomass_loss_2002") as "mangrove_biomass_loss_2002",
              sum("mangrove_biomass_loss_2003") as "mangrove_biomass_loss_2003",
              sum("mangrove_biomass_loss_2004") as "mangrove_biomass_loss_2004",
              sum("mangrove_biomass_loss_2005") as "mangrove_biomass_loss_2005",
              sum("mangrove_biomass_loss_2006") as "mangrove_biomass_loss_2006",
              sum("mangrove_biomass_loss_2007") as "mangrove_biomass_loss_2007",
              sum("mangrove_biomass_loss_2008") as "mangrove_biomass_loss_2008",
              sum("mangrove_biomass_loss_2009") as "mangrove_biomass_loss_2009",
              sum("mangrove_biomass_loss_2010") as "mangrove_biomass_loss_2010",
              sum("mangrove_biomass_loss_2011") as "mangrove_biomass_loss_2011",
              sum("mangrove_biomass_loss_2012") as "mangrove_biomass_loss_2012",
              sum("mangrove_biomass_loss_2013") as "mangrove_biomass_loss_2013",
              sum("mangrove_biomass_loss_2014") as "mangrove_biomass_loss_2014",
              sum("mangrove_biomass_loss_2015") as "mangrove_biomass_loss_2015",
              sum("mangrove_biomass_loss_2016") as "mangrove_biomass_loss_2016",
              sum("mangrove_biomass_loss_2017") as "mangrove_biomass_loss_2017",
              sum("mangrove_biomass_loss_2018") as "mangrove_biomass_loss_2018",
              sum("mangrove_carbon_emissions_2001") as "mangrove_carbon_emissions_2001",
              sum("mangrove_carbon_emissions_2002") as "mangrove_carbon_emissions_2002",
              sum("mangrove_carbon_emissions_2003") as "mangrove_carbon_emissions_2003",
              sum("mangrove_carbon_emissions_2004") as "mangrove_carbon_emissions_2004",
              sum("mangrove_carbon_emissions_2005") as "mangrove_carbon_emissions_2005",
              sum("mangrove_carbon_emissions_2006") as "mangrove_carbon_emissions_2006",
              sum("mangrove_carbon_emissions_2007") as "mangrove_carbon_emissions_2007",
              sum("mangrove_carbon_emissions_2008") as "mangrove_carbon_emissions_2008",
              sum("mangrove_carbon_emissions_2009") as "mangrove_carbon_emissions_2009",
              sum("mangrove_carbon_emissions_2010") as "mangrove_carbon_emissions_2010",
              sum("mangrove_carbon_emissions_2011") as "mangrove_carbon_emissions_2011",
              sum("mangrove_carbon_emissions_2012") as "mangrove_carbon_emissions_2012",
              sum("mangrove_carbon_emissions_2013") as "mangrove_carbon_emissions_2013",
              sum("mangrove_carbon_emissions_2014") as "mangrove_carbon_emissions_2014",
              sum("mangrove_carbon_emissions_2015") as "mangrove_carbon_emissions_2015",
              sum("mangrove_carbon_emissions_2016") as "mangrove_carbon_emissions_2016",
              sum("mangrove_carbon_emissions_2017") as "mangrove_carbon_emissions_2017",
              sum("mangrove_carbon_emissions_2018") as "mangrove_carbon_emissions_2018"
            )

          val tAnnualLossDF = sAnnualLossDF
            .join(
              tAdm2DF,
              $"feature_id" <=> $"m_feature_id"
                && $"layers" <=> $"m_layers" && $"threshold" <=> $"m_threshold",
              "right_outer"
            )
            .na
            .fill(
              0.0,
              Seq(
                "area",
                "biomass",
                "co2",
                "mangrove_biomass",
                "mangrove_co2",
                "area_loss_2001",
                "area_loss_2002",
                "area_loss_2003",
                "area_loss_2004",
                "area_loss_2005",
                "area_loss_2006",
                "area_loss_2007",
                "area_loss_2008",
                "area_loss_2009",
                "area_loss_2010",
                "area_loss_2011",
                "area_loss_2012",
                "area_loss_2013",
                "area_loss_2014",
                "area_loss_2015",
                "area_loss_2016",
                "area_loss_2017",
                "area_loss_2018",
                "biomass_loss_2001",
                "biomass_loss_2002",
                "biomass_loss_2003",
                "biomass_loss_2004",
                "biomass_loss_2005",
                "biomass_loss_2006",
                "biomass_loss_2007",
                "biomass_loss_2008",
                "biomass_loss_2009",
                "biomass_loss_2010",
                "biomass_loss_2011",
                "biomass_loss_2012",
                "biomass_loss_2013",
                "biomass_loss_2014",
                "biomass_loss_2015",
                "biomass_loss_2016",
                "biomass_loss_2017",
                "biomass_loss_2018",
                "carbon_emissions_2001",
                "carbon_emissions_2002",
                "carbon_emissions_2003",
                "carbon_emissions_2004",
                "carbon_emissions_2005",
                "carbon_emissions_2006",
                "carbon_emissions_2007",
                "carbon_emissions_2008",
                "carbon_emissions_2009",
                "carbon_emissions_2010",
                "carbon_emissions_2011",
                "carbon_emissions_2012",
                "carbon_emissions_2013",
                "carbon_emissions_2014",
                "carbon_emissions_2015",
                "carbon_emissions_2016",
                "carbon_emissions_2017",
                "carbon_emissions_2018",
                "mangrove_biomass_loss_2001",
                "mangrove_biomass_loss_2002",
                "mangrove_biomass_loss_2003",
                "mangrove_biomass_loss_2004",
                "mangrove_biomass_loss_2005",
                "mangrove_biomass_loss_2006",
                "mangrove_biomass_loss_2007",
                "mangrove_biomass_loss_2008",
                "mangrove_biomass_loss_2009",
                "mangrove_biomass_loss_2010",
                "mangrove_biomass_loss_2011",
                "mangrove_biomass_loss_2012",
                "mangrove_biomass_loss_2013",
                "mangrove_biomass_loss_2014",
                "mangrove_biomass_loss_2015",
                "mangrove_biomass_loss_2016",
                "mangrove_biomass_loss_2017",
                "mangrove_biomass_loss_2018",
                "mangrove_carbon_emissions_2001",
                "mangrove_carbon_emissions_2002",
                "mangrove_carbon_emissions_2003",
                "mangrove_carbon_emissions_2004",
                "mangrove_carbon_emissions_2005",
                "mangrove_carbon_emissions_2006",
                "mangrove_carbon_emissions_2007",
                "mangrove_carbon_emissions_2008",
                "mangrove_carbon_emissions_2009",
                "mangrove_carbon_emissions_2010",
                "mangrove_carbon_emissions_2011",
                "mangrove_carbon_emissions_2012",
                "mangrove_carbon_emissions_2013",
                "mangrove_carbon_emissions_2014",
                "mangrove_carbon_emissions_2015",
                "mangrove_carbon_emissions_2016",
                "mangrove_carbon_emissions_2017",
                "mangrove_carbon_emissions_2018"
              )
            )
            .na
            .fill(2001, Seq("year_2001"))
            .na
            .fill(2002, Seq("year_2002"))
            .na
            .fill(2003, Seq("year_2003"))
            .na
            .fill(2004, Seq("year_2004"))
            .na
            .fill(2005, Seq("year_2005"))
            .na
            .fill(2006, Seq("year_2006"))
            .na
            .fill(2007, Seq("year_2007"))
            .na
            .fill(2008, Seq("year_2008"))
            .na
            .fill(2009, Seq("year_2009"))
            .na
            .fill(2010, Seq("year_2010"))
            .na
            .fill(2011, Seq("year_2011"))
            .na
            .fill(2012, Seq("year_2012"))
            .na
            .fill(2013, Seq("year_2013"))
            .na
            .fill(2014, Seq("year_2014"))
            .na
            .fill(2015, Seq("year_2015"))
            .na
            .fill(2016, Seq("year_2016"))
            .na
            .fill(2017, Seq("year_2017"))
            .na
            .fill(2018, Seq("year_2018"))
            .select(
              $"m_feature_id" as "feature_id",
              $"m_threshold" as "threshold",
              $"m_layers" as "layers",
              $"totalarea",
              windowSum("area") as "extent2000",
              windowSum("biomass") as "total_biomass",
              windowSum("co2") as "total_co2",
              windowSum("mangrove_biomass") as "total_mangrove_biomass",
              windowSum("mangrove_co2") as "total_mangrove_co2",
              $"year_2001",
              $"year_2002",
              $"year_2003",
              $"year_2004",
              $"year_2005",
              $"year_2006",
              $"year_2007",
              $"year_2008",
              $"year_2009",
              $"year_2010",
              $"year_2011",
              $"year_2012",
              $"year_2013",
              $"year_2014",
              $"year_2015",
              $"year_2016",
              $"year_2017",
              $"year_2018",
              windowSum("area_loss_2001") as "area_loss_2001",
              windowSum("area_loss_2002") as "area_loss_2002",
              windowSum("area_loss_2003") as "area_loss_2003",
              windowSum("area_loss_2004") as "area_loss_2004",
              windowSum("area_loss_2005") as "area_loss_2005",
              windowSum("area_loss_2006") as "area_loss_2006",
              windowSum("area_loss_2007") as "area_loss_2007",
              windowSum("area_loss_2008") as "area_loss_2008",
              windowSum("area_loss_2009") as "area_loss_2009",
              windowSum("area_loss_2010") as "area_loss_2010",
              windowSum("area_loss_2011") as "area_loss_2011",
              windowSum("area_loss_2012") as "area_loss_2012",
              windowSum("area_loss_2013") as "area_loss_2013",
              windowSum("area_loss_2014") as "area_loss_2014",
              windowSum("area_loss_2015") as "area_loss_2015",
              windowSum("area_loss_2016") as "area_loss_2016",
              windowSum("area_loss_2017") as "area_loss_2017",
              windowSum("area_loss_2018") as "area_loss_2018",
              windowSum("biomass_loss_2001") as "biomass_loss_2001",
              windowSum("biomass_loss_2002") as "biomass_loss_2002",
              windowSum("biomass_loss_2003") as "biomass_loss_2003",
              windowSum("biomass_loss_2004") as "biomass_loss_2004",
              windowSum("biomass_loss_2005") as "biomass_loss_2005",
              windowSum("biomass_loss_2006") as "biomass_loss_2006",
              windowSum("biomass_loss_2007") as "biomass_loss_2007",
              windowSum("biomass_loss_2008") as "biomass_loss_2008",
              windowSum("biomass_loss_2009") as "biomass_loss_2009",
              windowSum("biomass_loss_2010") as "biomass_loss_2010",
              windowSum("biomass_loss_2011") as "biomass_loss_2011",
              windowSum("biomass_loss_2012") as "biomass_loss_2012",
              windowSum("biomass_loss_2013") as "biomass_loss_2013",
              windowSum("biomass_loss_2014") as "biomass_loss_2014",
              windowSum("biomass_loss_2015") as "biomass_loss_2015",
              windowSum("biomass_loss_2016") as "biomass_loss_2016",
              windowSum("biomass_loss_2017") as "biomass_loss_2017",
              windowSum("biomass_loss_2018") as "biomass_loss_2018",
              windowSum("carbon_emissions_2001") as "carbon_emissions_2001",
              windowSum("carbon_emissions_2002") as "carbon_emissions_2002",
              windowSum("carbon_emissions_2003") as "carbon_emissions_2003",
              windowSum("carbon_emissions_2004") as "carbon_emissions_2004",
              windowSum("carbon_emissions_2005") as "carbon_emissions_2005",
              windowSum("carbon_emissions_2006") as "carbon_emissions_2006",
              windowSum("carbon_emissions_2007") as "carbon_emissions_2007",
              windowSum("carbon_emissions_2008") as "carbon_emissions_2008",
              windowSum("carbon_emissions_2009") as "carbon_emissions_2009",
              windowSum("carbon_emissions_2010") as "carbon_emissions_2010",
              windowSum("carbon_emissions_2011") as "carbon_emissions_2011",
              windowSum("carbon_emissions_2012") as "carbon_emissions_2012",
              windowSum("carbon_emissions_2013") as "carbon_emissions_2013",
              windowSum("carbon_emissions_2014") as "carbon_emissions_2014",
              windowSum("carbon_emissions_2015") as "carbon_emissions_2015",
              windowSum("carbon_emissions_2016") as "carbon_emissions_2016",
              windowSum("carbon_emissions_2017") as "carbon_emissions_2017",
              windowSum("carbon_emissions_2018") as "carbon_emissions_2018",
              windowSum("mangrove_biomass_loss_2001") as "mangrove_biomass_loss_2001",
              windowSum("mangrove_biomass_loss_2002") as "mangrove_biomass_loss_2002",
              windowSum("mangrove_biomass_loss_2003") as "mangrove_biomass_loss_2003",
              windowSum("mangrove_biomass_loss_2004") as "mangrove_biomass_loss_2004",
              windowSum("mangrove_biomass_loss_2005") as "mangrove_biomass_loss_2005",
              windowSum("mangrove_biomass_loss_2006") as "mangrove_biomass_loss_2006",
              windowSum("mangrove_biomass_loss_2007") as "mangrove_biomass_loss_2007",
              windowSum("mangrove_biomass_loss_2008") as "mangrove_biomass_loss_2008",
              windowSum("mangrove_biomass_loss_2009") as "mangrove_biomass_loss_2009",
              windowSum("mangrove_biomass_loss_2010") as "mangrove_biomass_loss_2010",
              windowSum("mangrove_biomass_loss_2011") as "mangrove_biomass_loss_2011",
              windowSum("mangrove_biomass_loss_2012") as "mangrove_biomass_loss_2012",
              windowSum("mangrove_biomass_loss_2013") as "mangrove_biomass_loss_2013",
              windowSum("mangrove_biomass_loss_2014") as "mangrove_biomass_loss_2014",
              windowSum("mangrove_biomass_loss_2015") as "mangrove_biomass_loss_2015",
              windowSum("mangrove_biomass_loss_2016") as "mangrove_biomass_loss_2016",
              windowSum("mangrove_biomass_loss_2017") as "mangrove_biomass_loss_2017",
              windowSum("mangrove_biomass_loss_2018") as "mangrove_biomass_loss_2018",
              windowSum("mangrove_carbon_emissions_2001") as "mangrove_carbon_emissions_2001",
              windowSum("mangrove_carbon_emissions_2002") as "mangrove_carbon_emissions_2002",
              windowSum("mangrove_carbon_emissions_2003") as "mangrove_carbon_emissions_2003",
              windowSum("mangrove_carbon_emissions_2004") as "mangrove_carbon_emissions_2004",
              windowSum("mangrove_carbon_emissions_2005") as "mangrove_carbon_emissions_2005",
              windowSum("mangrove_carbon_emissions_2006") as "mangrove_carbon_emissions_2006",
              windowSum("mangrove_carbon_emissions_2007") as "mangrove_carbon_emissions_2007",
              windowSum("mangrove_carbon_emissions_2008") as "mangrove_carbon_emissions_2008",
              windowSum("mangrove_carbon_emissions_2009") as "mangrove_carbon_emissions_2009",
              windowSum("mangrove_carbon_emissions_2010") as "mangrove_carbon_emissions_2010",
              windowSum("mangrove_carbon_emissions_2011") as "mangrove_carbon_emissions_2011",
              windowSum("mangrove_carbon_emissions_2012") as "mangrove_carbon_emissions_2012",
              windowSum("mangrove_carbon_emissions_2013") as "mangrove_carbon_emissions_2013",
              windowSum("mangrove_carbon_emissions_2014") as "mangrove_carbon_emissions_2014",
              windowSum("mangrove_carbon_emissions_2015") as "mangrove_carbon_emissions_2015",
              windowSum("mangrove_carbon_emissions_2016") as "mangrove_carbon_emissions_2016",
              windowSum("mangrove_carbon_emissions_2017") as "mangrove_carbon_emissions_2017",
              windowSum("mangrove_carbon_emissions_2018") as "mangrove_carbon_emissions_2018"
            )

          val lookup2010 = Map("threshold_2010" -> "threshold")

          val s2010DF = summaryDF
            .select(
              summaryDF.columns
                .map(c => col(c).as(lookup2010.getOrElse(c, c))): _*
            )
            .groupBy("feature_id", "layers", "threshold")
            .agg(sum("area") as "area")
            .join(
              tAdm2DF,
              $"m_feature_id" <=> $"feature_id" &&
                $"m_layers" <=> $"layers" &&
                $"m_threshold" <=> $"threshold",
              "right_outer"
            )
            .na
            .fill(0.0, Seq("area"))
            .select(
              $"m_feature_id",
              $"m_layers",
              $"m_threshold",
              windowSum("area") as "extent2010"
            )

          val adm2DF = tAnnualLossDF
            .join(
              s2010DF,
              $"m_feature_id" <=> $"feature_id" &&
                $"m_layers" <=> $"layers" &&
                $"m_threshold" <=> $"threshold",
              "left_outer"
            )
            .select(
              $"feature_id.iso" as "iso",
              $"feature_id.adm1" as "adm1",
              $"feature_id.adm2" as "adm2",
              $"threshold",
              $"layers.drivers" as "tcs",
              $"layers.globalLandCover" as "global_land_cover",
              $"layers.primaryForest" as "primary_forest",
              $"layers.idnPrimaryForest" as "idn_primary_forest",
              $"layers.erosion" as "erosion",
              $"layers.biodiversitySignificance" as "biodiversity_significance",
              $"layers.biodiversityIntactness" as "biodiversity_intactness",
              $"layers.wdpa" as "wdpa",
              $"layers.aze" as "aze",
              $"layers.plantations" as "plantations",
              $"layers.riverBasins" as "river_basin",
              $"layers.ecozones" as "ecozone",
              $"layers.urbanWatersheds" as "urban_watershed",
              $"layers.mangroves1996" as "mangroves_1996",
              $"layers.mangroves2016" as "mangroves_2016",
              $"layers.waterStress" as "water_stress",
              $"layers.intactForestLandscapes" as "ifl",
              $"layers.endemicBirdAreas" as "endemic_bird_area",
              $"layers.tigerLandscapes" as "tiger_cl",
              $"layers.landmark" as "landmark",
              $"layers.landRights" as "land_right",
              $"layers.keyBiodiversityAreas" as "kba",
              $"layers.mining" as "mining",
              $"layers.rspo" as "rspo",
              $"layers.peatlands" as "idn_mys_peatlands",
              $"layers.oilPalm" as "oil_palm",
              $"layers.idnForestMoratorium" as "idn_forest_moratorium",
              $"layers.idnLandCover" as "idn_land_cover",
              $"layers.mexProtectedAreas" as "mex_protected_areas",
              $"layers.mexPaymentForEcosystemServices" as "mex_pes",
              $"layers.mexForestZoning" as "mex_forest_zoning",
              $"layers.perProductionForest" as "per_production_forest",
              $"layers.perProtectedAreas" as "per_protected_area",
              $"layers.perForestConcessions" as "per_forest_concession",
              $"layers.braBiomes" as "bra_biomes",
              $"layers.woodFiber" as "wood_fiber",
              $"layers.resourceRights" as "resource_right",
              $"layers.logging" as "managed_forests",
              $"layers.oilGas" as "oil_gas",
              $"totalarea",
              $"extent2000",
              $"extent2010",
              $"total_biomass",
              $"total_co2",
              $"total_mangrove_biomass",
              $"total_mangrove_co2",
              $"year_2001",
              $"area_loss_2001",
              $"biomass_loss_2001",
              $"carbon_emissions_2001",
              $"mangrove_biomass_loss_2001",
              $"mangrove_carbon_emissions_2001",
              $"year_2002",
              $"area_loss_2002",
              $"biomass_loss_2002",
              $"carbon_emissions_2002",
              $"mangrove_biomass_loss_2002",
              $"mangrove_carbon_emissions_2002",
              $"year_2003",
              $"area_loss_2003",
              $"biomass_loss_2003",
              $"carbon_emissions_2003",
              $"mangrove_biomass_loss_2003",
              $"mangrove_carbon_emissions_2003",
              $"year_2004",
              $"area_loss_2004",
              $"biomass_loss_2004",
              $"carbon_emissions_2004",
              $"mangrove_biomass_loss_2004",
              $"mangrove_carbon_emissions_2004",
              $"year_2005",
              $"area_loss_2005",
              $"biomass_loss_2005",
              $"carbon_emissions_2005",
              $"mangrove_biomass_loss_2005",
              $"mangrove_carbon_emissions_2005",
              $"year_2006",
              $"area_loss_2006",
              $"biomass_loss_2006",
              $"carbon_emissions_2006",
              $"mangrove_biomass_loss_2006",
              $"mangrove_carbon_emissions_2006",
              $"year_2007",
              $"area_loss_2007",
              $"biomass_loss_2007",
              $"carbon_emissions_2007",
              $"mangrove_biomass_loss_2007",
              $"mangrove_carbon_emissions_2007",
              $"year_2008",
              $"area_loss_2008",
              $"biomass_loss_2008",
              $"carbon_emissions_2008",
              $"mangrove_biomass_loss_2008",
              $"mangrove_carbon_emissions_2008",
              $"year_2009",
              $"area_loss_2009",
              $"biomass_loss_2009",
              $"carbon_emissions_2009",
              $"mangrove_biomass_loss_2009",
              $"mangrove_carbon_emissions_2009",
              $"year_2010",
              $"area_loss_2010",
              $"biomass_loss_2010",
              $"carbon_emissions_2010",
              $"mangrove_biomass_loss_2010",
              $"mangrove_carbon_emissions_2010",
              $"year_2011",
              $"area_loss_2011",
              $"biomass_loss_2011",
              $"carbon_emissions_2011",
              $"mangrove_biomass_loss_2011",
              $"mangrove_carbon_emissions_2011",
              $"year_2012",
              $"area_loss_2012",
              $"biomass_loss_2012",
              $"carbon_emissions_2012",
              $"mangrove_biomass_loss_2012",
              $"mangrove_carbon_emissions_2012",
              $"year_2013",
              $"area_loss_2013",
              $"biomass_loss_2013",
              $"carbon_emissions_2013",
              $"mangrove_biomass_loss_2013",
              $"mangrove_carbon_emissions_2013",
              $"year_2014",
              $"area_loss_2014",
              $"biomass_loss_2014",
              $"carbon_emissions_2014",
              $"mangrove_biomass_loss_2014",
              $"mangrove_carbon_emissions_2014",
              $"year_2015",
              $"area_loss_2015",
              $"biomass_loss_2015",
              $"carbon_emissions_2015",
              $"mangrove_biomass_loss_2015",
              $"mangrove_carbon_emissions_2015",
              $"year_2016",
              $"area_loss_2016",
              $"biomass_loss_2016",
              $"carbon_emissions_2016",
              $"mangrove_biomass_loss_2016",
              $"mangrove_carbon_emissions_2016",
              $"year_2017",
              $"area_loss_2017",
              $"biomass_loss_2017",
              $"carbon_emissions_2017",
              $"mangrove_biomass_loss_2017",
              $"mangrove_carbon_emissions_2017",
              $"year_2018",
              $"area_loss_2018",
              $"biomass_loss_2018",
              $"carbon_emissions_2018",
              $"mangrove_biomass_loss_2018",
              $"mangrove_carbon_emissions_2018"
            )
          //        .select($"feature_id",
          //          $"threshold",
          //          $"layers",
          //          $"totalarea",
          //          $"extent2000",
          //          $"extent2010",
          //          $"total_biomass",
          //          $"total_co2",
          //          $"total_mangrove_biomass",
          //          $"total_mangrove_co2",
          //          array(
          //            struct($"year_2001" as "year",
          //              $"area_loss_2001" as "area_loss",
          //              $"biomass_loss_2001" as "biomass_loss",
          //              $"carbon_emissions_2001" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2001" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2001" as "mangrove_carbon_emissions"),
          //            struct($"year_2002" as "year",
          //              $"area_loss_2002" as "area_loss",
          //              $"biomass_loss_2002" as "biomass_loss",
          //              $"carbon_emissions_2002" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2002" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2002" as "mangrove_carbon_emissions"),
          //            struct($"year_2003" as "year",
          //              $"area_loss_2003" as "area_loss",
          //              $"biomass_loss_2003" as "biomass_loss",
          //              $"carbon_emissions_2003" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2003" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2003" as "mangrove_carbon_emissions"),
          //            struct($"year_2004" as "year",
          //              $"area_loss_2004" as "area_loss",
          //              $"biomass_loss_2004" as "biomass_loss",
          //              $"carbon_emissions_2004" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2004" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2004" as "mangrove_carbon_emissions"),
          //            struct($"year_2005" as "year",
          //              $"area_loss_2005" as "area_loss",
          //              $"biomass_loss_2005" as "biomass_loss",
          //              $"carbon_emissions_2005" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2005" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2005" as "mangrove_carbon_emissions"),
          //            struct($"year_2006" as "year",
          //              $"area_loss_2006" as "area_loss",
          //              $"biomass_loss_2006" as "biomass_loss",
          //              $"carbon_emissions_2006" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2006" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2006" as "mangrove_carbon_emissions"),
          //            struct($"year_2007" as "year",
          //              $"area_loss_2007" as "area_loss",
          //              $"biomass_loss_2007" as "biomass_loss",
          //              $"carbon_emissions_2007" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2007" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2007" as "mangrove_carbon_emissions"),
          //            struct($"year_2008" as "year",
          //              $"area_loss_2008" as "area_loss",
          //              $"biomass_loss_2008" as "biomass_loss",
          //              $"carbon_emissions_2008" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2008" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2008" as "mangrove_carbon_emissions"),
          //            struct($"year_2009" as "year",
          //              $"area_loss_2009" as "area_loss",
          //              $"biomass_loss_2009" as "biomass_loss",
          //              $"carbon_emissions_2009" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2009" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2009" as "mangrove_carbon_emissions"),
          //            struct($"year_2010" as "year",
          //              $"area_loss_2010" as "area_loss",
          //              $"biomass_loss_2010" as "biomass_loss",
          //              $"carbon_emissions_2010" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2010" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2010" as "mangrove_carbon_emissions"),
          //            struct($"year_2011" as "year",
          //              $"area_loss_2011" as "area_loss",
          //              $"biomass_loss_2011" as "biomass_loss",
          //              $"carbon_emissions_2011" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2011" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2011" as "mangrove_carbon_emissions"),
          //            struct($"year_2012" as "year",
          //              $"area_loss_2012" as "area_loss",
          //              $"biomass_loss_2012" as "biomass_loss",
          //              $"carbon_emissions_2012" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2012" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2012" as "mangrove_carbon_emissions"),
          //            struct($"year_2013" as "year",
          //              $"area_loss_2013" as "area_loss",
          //              $"biomass_loss_2013" as "biomass_loss",
          //              $"carbon_emissions_2013" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2013" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2013" as "mangrove_carbon_emissions"),
          //            struct($"year_2014" as "year",
          //              $"area_loss_2014" as "area_loss",
          //              $"biomass_loss_2014" as "biomass_loss",
          //              $"carbon_emissions_2014" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2014" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2014" as "mangrove_carbon_emissions"),
          //            struct($"year_2015" as "year",
          //              $"area_loss_2015" as "area_loss",
          //              $"biomass_loss_2015" as "biomass_loss",
          //              $"carbon_emissions_2015" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2015" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2015" as "mangrove_carbon_emissions"),
          //            struct($"year_2016" as "year",
          //              $"area_loss_2016" as "area_loss",
          //              $"biomass_loss_2016" as "biomass_loss",
          //              $"carbon_emissions_2016" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2016" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2016" as "mangrove_carbon_emissions"),
          //            struct($"year_2017" as "year",
          //              $"area_loss_2017" as "area_loss",
          //              $"biomass_loss_2017" as "biomass_loss",
          //              $"carbon_emissions_2017" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2017" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2017" as "mangrove_carbon_emissions"),
          //            struct($"year_2018" as "year",
          //              $"area_loss_2018" as "area_loss",
          //              $"biomass_loss_2018" as "biomass_loss",
          //              $"carbon_emissions_2018" as "carbon_emissions",
          //              $"mangrove_biomass_loss_2018" as "mangrove_biomass_loss",
          //              $"mangrove_carbon_emissions_2018" as "mangrove_carbon_emissions")
          //          ) as "year_data")

          val outputPartitionCount =
            maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

          adm2DF.show()

          adm2DF.
            //        select($"feature_id.iso".alias("iso"),
            //          $"feature_id.adm1".alias("adm1"),
            //          $"feature_id.adm2".alias("adm2"),
            //          $"threshold",
            //          $"layers.drivers".alias("tcs"),
            //          $"layers.globalLandCover".alias("global_land_cover"),
            //          $"layers.primaryForest".alias("primary_forest"),
            //          $"layers.idnPrimaryForest".alias("idn_primary_forest"),
            //          $"layers.erosion".alias("erosion"),
            //          $"layers.biodiversitySignificance".alias("biodiversity_significance"),
            //          $"layers.biodiversityIntactness".alias("biodiversity_intactness"),
            //          $"layers.wdpa".alias("wdpa"),
            //          $"layers.aze".alias("aze"),
            //          $"layers.plantations".alias("plantations"),
            //          $"layers.riverBasins".alias("river_basin"),
            //          $"layers.ecozones".alias("ecozone"),
            //          $"layers.urbanWatersheds".alias("urban_watershed"),
            //          $"layers.mangroves1996".alias("mangroves_1996"),
            //          $"layers.mangroves2016".alias("mangroves_2016"),
            //          $"layers.waterStress".alias("water_stress"),
            //          $"layers.intactForestLandscapes".alias("ifl"),
            //          $"layers.endemicBirdAreas".alias("endemic_bird_area"),
            //          $"layers.tigerLandscapes".alias("tiger_cl"),
            //          $"layers.landmark".alias("landmark"),
            //          $"layers.landRights".alias("land_right"),
            //          $"layers.keyBiodiversityAreas".alias("kba"),
            //          $"layers.mining".alias("mining"),
            //          $"layers.rspo".alias("rspo"),
            //          $"layers.peatlands".alias("idn_mys_peatlands"),
            //          $"layers.oilPalm".alias("oil_palm"),
            //          $"layers.idnForestMoratorium".alias("idn_forest_moratorium"),
            //          $"layers.idnLandCover".alias("idn_land_cover"),
            //          $"layers.mexProtectedAreas".alias("mex_protected_areas"),
            //          $"layers.mexPaymentForEcosystemServices".alias("mex_pes"),
            //          $"layers.mexForestZoning".alias("mex_forest_zoning"),
            //          $"layers.perProductionForest".alias("per_production_forest"),
            //          $"layers.perProtectedAreas".alias("per_protected_area"),
            //          $"layers.perForestConcessions".alias("per_forest_concession"),
            //          $"layers.braBiomes".alias("bra_biomes"),
            //          $"layers.woodFiber".alias("wood_fiber"),
            //          $"layers.resourceRights".alias("resource_right"),
            //          $"layers.logging".alias("managed_forests"),
            //          $"layers.oilGas".alias("oil_gas"),
            //          $"totalarea",
            //          $"extent2000",
            //          $"extent2010",
            //          $"total_biomass", $"total_co2", $"total_mangrove_biomass", $"total_mangrove_co2", $"year_data"
            //        ).
            repartition(1).write
            .options(
              Map(
                "header" -> "true",
                "delimiter" -> "\t",
                "quote" -> "\u0000",
                "quoteMode" -> "NONE",
                "nullValue" -> "\u0000"
              )
            )
            . // unicode for nothing. tried "quoteMode" -> "NONE" but didn't work
            csv(path = outputUrl + "/adm2")

          //      val jsonDs = summaryDF.repartition(outputPartitionCount).toJSON
          //            finalDF.
          //              repartition(1).
          //        toJSON. //.repartition(outputPartitionCount).toJSON
          //        mapPartitions(vals => Iterator("[" + vals.mkString(",") + "]")).
          //        write.
          //        text(outputUrl)

          //      val adm1DF = adm2DF.groupBy("feature_id.iso", "feature_id.adm1", "layers", "threshold")
          //        .agg(sum("totalarea") as "totalarea", sum("extent2000") as "extent2000", sum("extent2010") as "extent2010",
          //          sum("total_biomass") as "total_biomass", sum("total_co2") as "total_co2", sum("total_mangrove_biomass") as "total_mangrove_biomass", sum("total_mangrove_co2") as "total_mangrove_co2")

          //
          //      adm1DF.
          //        select($"iso",
          //          $"adm1",
          //          $"threshold",
          //          $"layers.drivers".alias("tcs"),
          //          $"layers.globalLandCover".alias("global_land_cover"),
          //          $"layers.primaryForest".alias("primary_forest"),
          //          $"layers.idnPrimaryForest".alias("idn_primary_forest"),
          //          $"layers.erosion".alias("erosion"),
          //          $"layers.biodiversitySignificance".alias("biodiversity_significance"),
          //          $"layers.biodiversityIntactness".alias("biodiversity_intactness"),
          //          $"layers.wdpa".alias("wdpa"),
          //          $"layers.aze".alias("aze"),
          //          $"layers.plantations".alias("plantations"),
          //          $"layers.riverBasins".alias("river_basin"),
          //          $"layers.ecozones".alias("ecozone"),
          //          $"layers.urbanWatersheds".alias("urban_watershed"),
          //          $"layers.mangroves1996".alias("mangroves_1996"),
          //          $"layers.mangroves2016".alias("mangroves_2016"),
          //          $"layers.waterStress".alias("water_stress"),
          //          $"layers.intactForestLandscapes".alias("ifl"),
          //          $"layers.endemicBirdAreas".alias("endemic_bird_area"),
          //          $"layers.tigerLandscapes".alias("tiger_cl"),
          //          $"layers.landmark".alias("landmark"),
          //          $"layers.landRights".alias("land_right"),
          //          $"layers.keyBiodiversityAreas".alias("kba"),
          //          $"layers.mining".alias("mining"),
          //          $"layers.rspo".alias("rspo"),
          //          $"layers.peatlands".alias("idn_mys_peatlands"),
          //          $"layers.oilPalm".alias("oil_palm"),
          //          $"layers.idnForestMoratorium".alias("idn_forest_moratorium"),
          //          $"layers.idnLandCover".alias("idn_land_cover"),
          //          $"layers.mexProtectedAreas".alias("mex_protected_areas"),
          //          $"layers.mexPaymentForEcosystemServices".alias("mex_pes"),
          //          $"layers.mexForestZoning".alias("mex_forest_zoning"),
          //          $"layers.perProductionForest".alias("per_production_forest"),
          //          $"layers.perProtectedAreas".alias("per_protected_area"),
          //          $"layers.perForestConcessions".alias("per_forest_concession"),
          //          $"layers.braBiomes".alias("bra_biomes"),
          //          $"layers.woodFiber".alias("wood_fiber"),
          //          $"layers.resourceRights".alias("resource_right"),
          //          $"layers.logging".alias("managed_forests"),
          //          $"layers.oilGas".alias("oil_gas"),
          //          $"totalarea",
          //          $"extent2000",
          //          $"extent2010",
          //          $"total_biomass", $"total_co2", $"total_mangrove_biomass", $"total_mangrove_co2"
          //        ).
          //        repartition(1).
          //        write.options(Map("header" -> "true", "delimiter" -> "\t", "quote" -> "\u0000", "quoteMode" -> "NONE", "nullValue" -> "\u0000")). // unicode for nothing. tried "quoteMode" -> "NONE" but didn't work
          //        csv(path = outputUrl + "/adm1")
          //
          //      val isoDF = adm1DF.groupBy("iso", "layers", "threshold")
          //        .agg(sum("totalarea") as "totalarea", sum("extent2000") as "extent2000", sum("extent2010") as "extent2010",
          //          sum("total_biomass") as "total_biomass", sum("total_co2") as "total_co2", sum("total_mangrove_biomass") as "total_mangrove_biomass", sum("total_mangrove_co2") as "total_mangrove_co2")
          //
          //
          //      isoDF.
          //        select($"iso",
          //          $"threshold",
          //          $"layers.drivers".alias("tcs"),
          //          $"layers.globalLandCover".alias("global_land_cover"),
          //          $"layers.primaryForest".alias("primary_forest"),
          //          $"layers.idnPrimaryForest".alias("idn_primary_forest"),
          //          $"layers.erosion".alias("erosion"),
          //          $"layers.biodiversitySignificance".alias("biodiversity_significance"),
          //          $"layers.biodiversityIntactness".alias("biodiversity_intactness"),
          //          $"layers.wdpa".alias("wdpa"),
          //          $"layers.aze".alias("aze"),
          //          $"layers.plantations".alias("plantations"),
          //          $"layers.riverBasins".alias("river_basin"),
          //          $"layers.ecozones".alias("ecozone"),
          //          $"layers.urbanWatersheds".alias("urban_watershed"),
          //          $"layers.mangroves1996".alias("mangroves_1996"),
          //          $"layers.mangroves2016".alias("mangroves_2016"),
          //          $"layers.waterStress".alias("water_stress"),
          //          $"layers.intactForestLandscapes".alias("ifl"),
          //          $"layers.endemicBirdAreas".alias("endemic_bird_area"),
          //          $"layers.tigerLandscapes".alias("tiger_cl"),
          //          $"layers.landmark".alias("landmark"),
          //          $"layers.landRights".alias("land_right"),
          //          $"layers.keyBiodiversityAreas".alias("kba"),
          //          $"layers.mining".alias("mining"),
          //          $"layers.rspo".alias("rspo"),
          //          $"layers.peatlands".alias("idn_mys_peatlands"),
          //          $"layers.oilPalm".alias("oil_palm"),
          //          $"layers.idnForestMoratorium".alias("idn_forest_moratorium"),
          //          $"layers.idnLandCover".alias("idn_land_cover"),
          //          $"layers.mexProtectedAreas".alias("mex_protected_areas"),
          //          $"layers.mexPaymentForEcosystemServices".alias("mex_pes"),
          //          $"layers.mexForestZoning".alias("mex_forest_zoning"),
          //          $"layers.perProductionForest".alias("per_production_forest"),
          //          $"layers.perProtectedAreas".alias("per_protected_area"),
          //          $"layers.perForestConcessions".alias("per_forest_concession"),
          //          $"layers.braBiomes".alias("bra_biomes"),
          //          $"layers.woodFiber".alias("wood_fiber"),
          //          $"layers.resourceRights".alias("resource_right"),
          //          $"layers.logging".alias("managed_forests"),
          //          $"layers.oilGas".alias("oil_gas"),
          //          $"totalarea",
          //          $"extent2000",
          //          $"extent2010",
          //          $"total_biomass", $"total_co2", $"total_mangrove_biomass", $"total_mangrove_co2"
          //        ).
          //        repartition(1).
          //        write.options(Map("header" -> "true", "delimiter" -> "\t", "quote" -> "\u0000", "quoteMode" -> "NONE", "nullValue" -> "\u0000")). // unicode for nothing. tried "quoteMode" -> "NONE" but didn't work
          //        csv(path = outputUrl + "/iso")

          spark.stop
      }
    }
  )
