package usbuildings

import java.net.URL

import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.io._
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import cats.implicits._
import geotrellis.vector.io.wkt.WKT
import geotrellis.vector.{Feature, Geometry}

object TreeLossSummaryMain extends CommandApp (
  name = "geotrellis-tree-summary",
  header = "Collect building footprint elevations from terrain tiles",
  main = {
    val featuresOpt = Opts.options[String]("features", "URI of features in TSV format")
    val outputOpt = Opts.option[String]("output", "URI of output dir for CSV files")
    val limitOpt = Opts.option[Int]("limit", help = "Limit number of records processed").orNone

    val logger = Logger.getLogger("StatsMain")

    (featuresOpt, outputOpt, limitOpt).mapN { (featureUris, outputUrl, limit) =>
      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Tree Cover Loss Dataframe").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
      import spark.implicits._


      var featuresDF: DataFrame = spark.read.
        options(Map("header" -> "false", "delimiter" -> "\t")).
        csv(featureUris.toList: _*)

      limit.foreach( n => featuresDF = featuresDF.limit(n) )

      /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
      val featureRDD: RDD[Feature[Geometry, Id]] =
        featuresDF.rdd.map { row: Row =>
          val areaType = row.getString(1)
          val countryCode: String = row.getString(6)
          val admin1: Int = row.getString(7).toInt
          val admin2: Int = row.getString(8).toInt
          val geom: Geometry = WKT.read(row.getString(0))
          Feature(geom, Id(areaType, countryCode, admin1, admin2))
        }

      val partitioner = new HashPartitioner(partitions = featureRDD.getNumPartitions * 128)

      val summaryRDD: RDD[(Id, TreeLossSummary)] = TreeLossJob.apply(featureRDD, partitioner)

      val summaryDF =
        summaryRDD.flatMap { case (id, record) =>
          record.years.map { case (year, lossData) =>
            (id.country, id.areaType, id.admin1, id.admin2, year, lossData.tcd.mean(), lossData.totalCo2)
          }
        }.toDF("country", "area_type", "admin1", "admin2", "year", "tcd_mean", "biomass_sum")

      summaryDF.
        repartition(featureRDD.getNumPartitions).
        write.
          options(Map("header" -> "true", "delimiter" -> ",")).
          csv(path = outputUrl)

      spark.stop
    }
  }
)


