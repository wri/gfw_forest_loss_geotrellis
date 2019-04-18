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

            LossRow(LossRowFeatureId(id.country, admin1, admin2),
              lossDataGroup.tcd2000, lossDataGroup.tcd2010,
              LossRowLayers(
                lossDataGroup.drivers,
              lossDataGroup.globalLandCover, lossDataGroup.primaryForest,
              lossDataGroup.idnPrimaryForest, lossDataGroup.erosion, lossDataGroup.biodiversitySignificance, lossDataGroup.biodiversityIntactness,
              lossDataGroup.wdpa, lossDataGroup.aze, lossDataGroup.plantations,
              lossDataGroup.riverBasins, lossDataGroup.ecozones, lossDataGroup.urbanWatersheds,
              lossDataGroup.mangroves1996, lossDataGroup.mangroves2016, lossDataGroup.waterStress,
              lossDataGroup.intactForestLandscapes, lossDataGroup.endemicBirdAreas,
              lossDataGroup.tigerLandscapes, lossDataGroup.landmark, lossDataGroup.landRights,
              lossDataGroup.keyBiodiversityAreas, lossDataGroup.mining, lossDataGroup.rspo, lossDataGroup.peatlands,
              lossDataGroup.oilPalm, lossDataGroup.idnForestMoratorium, lossDataGroup.idnLandCover,
              lossDataGroup.mexProtectedAreas, lossDataGroup.mexPaymentForEcosystemServices,
              lossDataGroup.mexForestZoning, lossDataGroup.perProductionForest, lossDataGroup.perProtectedAreas,
              lossDataGroup.perForestConcessions, lossDataGroup.braBiomes, lossDataGroup.woodFiber,
                lossDataGroup.resourceRights, lossDataGroup.logging, lossDataGroup.oilGas),
              lossData.totalArea, lossData.totalGainArea,
              lossData.totalBiomass, lossData.totalCo2, lossData.biomassHistogram.mean(),
              lossData.totalMangroveBiomass, lossData.totalMangroveCo2, lossData.mangroveBiomassHistogram.mean(), LossYearDataMap.toList(lossData.lossYear)
            )
          }
          }
        }.toDF("feature_id",
          "threshold_2000", "threshold_2010", "layers",
          "area", "gain",
          "total_biomass", "total_co2", "mean_biomass_per_ha",
          "total_mangrove_biomass", "total_mangrove_co2", "mean_mangrove_biomass_per_ha", "year_data")

      var sparkConf: SparkConf = null
      sparkConf = new SparkConf().set("spark.sql.crossJoin.enabled", "true")

      val thresholdDF = Seq(0, 10, 15, 20, 25, 30, 50, 75).toDF("threshold")
      val tAdm2DF = summaryDF
        .groupBy("feature_id", "layers")
        .agg(sum("area") as "totalarea")
        .crossJoin(thresholdDF)


      val lookup2000 = Map("threshold_2000" -> "threshold")
      val s2000DF = summaryDF
        .select(summaryDF.columns.map(c => col(c).as(lookup2000.getOrElse(c, c))): _*)
        .groupBy("feature_id", "layers", "threshold")
        .agg(sum("area") as "area")
      val t2000DF = s2000DF
        .join(tAdm2DF, tAdm2DF("feature_id") <=> s2000DF("feature_id")
          && tAdm2DF("layers") <=> s2000DF("layers") && tAdm2DF("threshold") <=> s2000DF("threshold")
          , "right_outer")
        .select(tAdm2DF("feature_id"), tAdm2DF("threshold"), tAdm2DF("layers"), s2000DF("area"))

      val lookup2010 = Map("threshold_2010" -> "threshold")
      val s2010DF = summaryDF
        .select(summaryDF.columns.map(c => col(c).as(lookup2010.getOrElse(c, c))): _*)
        .groupBy("feature_id", "layers", "threshold")
        .agg(sum("area") as "area")
      val t2010DF = s2010DF
        .join(tAdm2DF, tAdm2DF("feature_id") <=> s2010DF("feature_id")
          && tAdm2DF("layers") <=> s2010DF("layers") && tAdm2DF("threshold") <=> s2010DF("threshold")
          , "right_outer")
        .select(tAdm2DF("feature_id"), tAdm2DF("threshold"), tAdm2DF("layers"), s2010DF("area"))


      val WindowPartitionOrder = Window.partitionBy($"feature_id", $"layers").orderBy($"threshold".desc)

      val tcExtent = sum("area").over(WindowPartitionOrder)
      val biomassSum = sum("total_biomass").over(WindowPartitionOrder)
      val co2Sum = sum("totalCo2").over(WindowPartitionOrder)

      val u2000DF = t2000DF.select($"*", tcExtent as "extent2000")
      val u2010DF = t2010DF.select($"*", tcExtent as "extent2010")

      val finalDF = tAdm2DF.
        join(u2000DF, tAdm2DF("feature_id") <=> u2000DF("feature_id")
          && tAdm2DF("layers") <=> u2000DF("layers") && tAdm2DF("threshold") <=> u2000DF("threshold")
          , "left_outer")
        .select(tAdm2DF("feature_id"), tAdm2DF("layers"), tAdm2DF("threshold"), $"totalarea", $"extent2000")
        .join(u2010DF, tAdm2DF("feature_id") <=> u2010DF("feature_id") && tAdm2DF("threshold") <=> u2010DF("threshold")
          && u2010DF("layers") <=> tAdm2DF("layers")
          , "left_outer")
        .select(tAdm2DF("feature_id"), tAdm2DF("threshold"), tAdm2DF("layers"), $"totalarea", $"extent2000", $"extent2010")
        .groupBy("feature_id", "threshold", "layers")
        .agg(sum("totalarea") as "totalarea", sum("extent2000") as "extent2000", sum("extent2010") as "extent2010")
        .select($"feature_id.iso".alias("iso"),
          $"feature_id.adm1".alias("adm1"),
          $"feature_id.adm2".alias("adm2"),
          $"threshold",
          $"layers.drivers".alias("tcs"),
          $"layers.globalLandCover".alias("global_land_cover"),
          $"layers.primaryForest".alias("primary_forest"),
          $"layers.idnPrimaryForest".alias("idn_primary_forest"),
          $"layers.erosion".alias("erosion"),
          $"layers.biodiversitySignificance".alias("biodiversity_significance"),
          $"layers.biodiversityIntactness".alias("biodiversity_intactness"),
          $"layers.wdpa".alias("wdpa"),
          $"layers.aze".alias("aze"),
          $"layers.plantations".alias("plantations"),
          $"layers.riverBasins".alias("river_basin"),
          $"layers.ecozones".alias("ecozone"),
          $"layers.urbanWatersheds".alias("urban_watershed"),
          $"layers.mangroves1996".alias("mangroves_1996"),
          $"layers.mangroves2016".alias("mangroves_2016"),
          $"layers.waterStress".alias("water_stress"),
          $"layers.intactForestLandscapes".alias("ifl"),
          $"layers.endemicBirdAreas".alias("endemic_bird_area"),
          $"layers.tigerLandscapes".alias("tiger_cl"),
          $"layers.landmark".alias("landmark"),
          $"layers.landRights".alias("land_right"),
          $"layers.keyBiodiversityAreas".alias("kba"),
          $"layers.mining".alias("mining"),
          $"layers.rspo".alias("rspo"),
          $"layers.peatlands".alias("idn_mys_peatlands"),
          $"layers.oilPalm".alias("oil_palm"),
          $"layers.idnForestMoratorium".alias("idn_forest_moratorium"),
          $"layers.idnLandCover".alias("idn_land_cover"),
          $"layers.mexProtectedAreas".alias("mex_protected_areas"),
          $"layers.mexPaymentForEcosystemServices".alias("mex_pes"),
          $"layers.mexForestZoning".alias("mex_forest_zoning"),
          $"layers.perProductionForest".alias("per_production_forest"),
          $"layers.perProtectedAreas".alias("per_protected_area"),
          $"layers.perForestConcessions".alias("per_forest_concession"),
          $"layers.braBiomes".alias("bra_biomes"),
          $"layers.woodFiber".alias("wood_fiber"),
          $"layers.resourceRights".alias("resource_right"),
          $"layers.logging".alias("managed_forests"),
          $"layers.oilGas".alias("oil_gas"),
          $"totalarea",
          $"extent2000",
          $"extent2010"
        )
      val outputPartitionCount = maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

      //val jsonDs = summaryDF.repartition(outputPartitionCount).toJSON
      //      finalDF.
      //        repartition(1).
      //        toJSON. //.repartition(outputPartitionCount).toJSON
      //        //mapPartitions(vals => Iterator("[" + vals.mkString(",") + "]")).
      //        write.
      //        text(outputUrl)


      finalDF.
        repartition(1).
        write.options(Map("header" -> "true", "delimiter" -> "\t", "quote" -> "\u0000", "quoteMode" -> "NONE", "nullValue" -> "\u0000")). // unicode for nothing. tried "quoteMode" -> "NONE" but didn't work
        csv(path = outputUrl)

      spark.stop
    }
  }
)
