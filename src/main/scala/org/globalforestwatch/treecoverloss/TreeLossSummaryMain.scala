package org.globalforestwatch.treecoverloss

import com.monovore.decline.{CommandApp, Opts}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import cats.implicits._
import geotrellis.vector.io.wkb.WKB
import geotrellis.vector.{Feature, Geometry}

object TreeLossSummaryMain extends CommandApp (
  name = "geotrellis-tree-cover-loss",
  header = "Compute statistics on tree cover loss",
  main = {

    val featuresOpt = Opts.options[String](
      "features", "URI of features in TSV format")

    val outputOpt = Opts.option[String](
      "output", "URI of output dir for CSV files")

    // Can be used to increase the level of job parallelism
    val intputPartitionsOpt = Opts.option[Int](
      "input-partitions", "Partition multiplier for input").withDefault(16)

    // Can be used to consolidate output into fewer files
    val outputPartitionsOpt = Opts.option[Int](
      "output-partitions", "Number of output partitions / files to be written").orNone

    val limitOpt = Opts.option[Int](
      "limit", help = "Limit number of records processed").orNone

    val isoOpt = Opts.option[String](
      "iso", help = "Filter by country ISO code").orNone

    val admin1Opt = Opts.option[String](
      "admin1", help = "Filter by country Admin1 code").orNone

    val admin2Opt = Opts.option[String](
      "admin2", help = "Filter by country Admin2 code").orNone

    val logger = Logger.getLogger("TreeLossSummaryMain")

    (
      featuresOpt, outputOpt, intputPartitionsOpt, outputPartitionsOpt, limitOpt, isoOpt, admin1Opt, admin2Opt
    ).mapN { (featureUris, outputUrl, inputPartitionMultiplier, maybeOutputPartitions, limit, iso, admin1, admin2) =>

      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Tree Cover Loss DataFrame").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
      import spark.implicits._

      // ref: https://github.com/databricks/spark-csv
      var featuresDF: DataFrame = spark.read.
        options(Map("header" -> "true", "delimiter" -> "\t")).
        csv(featureUris.toList: _*)

      iso.foreach { isoCode =>
        featuresDF = featuresDF.filter($"gid_0" === isoCode)
      }

      admin1.foreach { admin1Code =>
        featuresDF = featuresDF.filter($"gid_1" === admin1Code)
      }

      admin2.foreach { admin2Code =>
        featuresDF = featuresDF.filter($"gid_2" === admin2Code)
      }

      limit.foreach{ n =>
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

      val part = new HashPartitioner(partitions = featureRDD.getNumPartitions * inputPartitionMultiplier)

      val summaryRDD: RDD[(FeatureId, TreeLossSummary)] =
        TreeLossRDD(featureRDD, TenByTenGrid.blockTileGrid, part)

      val summaryDF =
        summaryRDD.flatMap { case (id, treeLossSummary) =>
          treeLossSummary.stats.map { case (lossDataGroup, lossData) => {

            val admin1 = if (id.admin1.length > 0) id.admin1.split("[.]")(1).split("[_]")(0) else ""
            val admin2 = if (id.admin2.length > 0) id.admin2.split("[.]")(2).split("[_]")(0) else ""

            LossRow(id.country, admin1, admin2,
              lossDataGroup.tcd2000, lossDataGroup.tcd2010, lossDataGroup.drivers,
              lossDataGroup.globalLandCover, lossDataGroup.primaryForest,
              lossDataGroup.idnPrimaryForest, lossDataGroup.erosion, lossDataGroup.biodiversitySignificance, lossDataGroup.biodiversityIntactness,
              lossDataGroup.wdpa, lossDataGroup.plantations,
              lossDataGroup.riverBasins, lossDataGroup.ecozones, lossDataGroup.urbanWatersheds,
              lossDataGroup.mangroves1996, lossDataGroup.mangroves2016, lossDataGroup.waterStress,
              lossDataGroup.intactForestLandscapes, lossDataGroup.endemicBirdAreas,
              lossDataGroup.tigerLandscapes, lossDataGroup.landmark, lossDataGroup.landRights,
              lossDataGroup.keyBiodiversityAreas, lossDataGroup.mining, lossDataGroup.rspo, lossDataGroup.peatlands,
              lossDataGroup.oilPalm, lossDataGroup.idnForestMoratorium, lossDataGroup.idnLandCover,
              lossDataGroup.mexProtectedAreas, lossDataGroup.mexPaymentForEcosystemServices,
              lossDataGroup.mexForestZoning, lossDataGroup.perProductionForest, lossDataGroup.perProtectedAreas,
              lossDataGroup.perForestConcessions, lossDataGroup.braBiomes, lossDataGroup.woodFiber,
              lossDataGroup.resourceRights, lossDataGroup.logging, lossDataGroup.oilGas,
              lossData.totalArea, lossData.totalGainArea,
              lossData.totalBiomass, lossData.totalCo2, lossData.biomassHistogram.mean(),
              lossData.totalMangroveBiomass, lossData.totalMangroveCo2, lossData.mangroveBiomassHistogram.mean(), LossYearData.toString(lossData.lossYear)
            )
          }
          }
        }.toDF("iso", "adm1", "adm2",
          "threshold_2000", "threshold_2010", "loss_driver", "global_land_cover", "primary_forest", "idn_primary_forest", "erosion",
          "biodiversity_significance", "biodiversity_intactness",
          "protected_area", "plantation", "river_basin",
          "ecozone", "urban_watershed", "mangroves_1996", "mangroves_2016", "water_stress", "intact_fores_landscape",
          "endemic_bird_area", "tiger_landscape", "landmark", "land_right", "key_biodiversity_area", "mining", "rspo",
          "peatland", "oil_palm", "idn_forest_moratorium", "idn_land_cover", "mex_protected_areas", "mex_pes",
          "mex_forest_zoning", "per_production_forest", "per_protected_area", "per_forest_concession", "bra_biomes",
          "wood_fiber", "resource_right", "logging", "oil_gas",
          "area", "gain",
          "total_biomass", "total_co2", "mean_biomass_per_ha",
          "total_mangrove_biomass", "total_mangrove_co2", "mean_mangrove_biomass_per_ha", "year_data")

      val outputPartitionCount = maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

      summaryDF.
        repartition(outputPartitionCount).
        write.
        options(Map("header" -> "true", "delimiter" -> "\t", "quote" -> "\u0000", "quoteMode" -> "NONE", "nullValue" -> "NULL")). // unicode for nothing. tried "quoteMode" -> "NONE" but didn't work
          csv(path = outputUrl)

      spark.stop
    }
  }
)
