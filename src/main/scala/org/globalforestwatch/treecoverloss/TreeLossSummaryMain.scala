package org.globalforestwatch.treecoverloss

import java.net.URL

import com.monovore.decline.{CommandApp, Opts}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import cats.implicits._
import geotrellis.vector.io.wkt.WKT
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

    val logger = Logger.getLogger("TreeLossSummaryMain")


    (
      featuresOpt, outputOpt, intputPartitionsOpt, outputPartitionsOpt, limitOpt
    ).mapN { (featureUris, outputUrl, inputPartitionMultiplier, maybeOutputPartitions, limit) =>

      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Tree Cover Loss DataFrame").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
      import spark.implicits._

      // ref: https://github.com/databricks/spark-csv
      var featuresDF: DataFrame = spark.read.
        options(Map("header" -> "false", "delimiter" -> "\t")).
        csv(featureUris.toList: _*)

      limit.foreach{ n =>
        featuresDF = featuresDF.limit(n)
      }

      /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
      val featureRDD: RDD[Feature[Geometry, FeatureId]] =
        featuresDF.rdd.map { row: Row =>
          val areaType = row.getString(1)
          val countryCode: String = row.getString(6)
          val admin1: Int = row.getString(7).toInt
          val admin2: Int = row.getString(8).toInt
          val geom: Geometry = WKT.read(row.getString(0))
          Feature(geom, FeatureId(areaType, countryCode, admin1, admin2))
        }

      val part = new HashPartitioner(partitions = featureRDD.getNumPartitions * inputPartitionMultiplier)

      val summaryRDD: RDD[(FeatureId, TreeLossSummary)] =
        TreeLossRDD(featureRDD, TenByTenGrid.stripedTileGrid, part)

      val summaryDF =
        summaryRDD.flatMap { case (id, treeLossSummary) =>
          treeLossSummary.years.map { case (year, lossData) =>
            (id.country, id.areaType, id.admin1, id.admin2, year, lossData.tcdHistogram.mean(), lossData.totalCo2)
          }
        }.toDF("country", "area_type", "admin1", "admin2", "year", "tcd_mean", "biomass_sum")

      val outputPartitionCount = maybeOutputPartitions.getOrElse(featureRDD.getNumPartitions)

      summaryDF.
        repartition(outputPartitionCount).
        write.
          options(Map("header" -> "true", "delimiter" -> ",")).
          csv(path = outputUrl)

      spark.stop
    }
  }
)
