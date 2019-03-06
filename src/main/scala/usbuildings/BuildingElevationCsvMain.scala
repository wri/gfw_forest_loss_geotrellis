package usbuildings

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import com.monovore.decline.{CommandApp, Opts}
import cats.implicits._

import org.apache.log4j.Logger
import geotrellis.vector.{Feature, Polygon}
import geotrellis.vector.io._
import geotrellis.vector.io.wkt.WKT
import org.apache.spark.rdd.RDD

object BuildingElevationCsvMain extends CommandApp (
  name = "geotrellis-usbuildings",
  header = "Collect building footprint elevations from terrain tiles",
  main = {
    val featuresOpt = Opts.option[String]("features", help = "URI of the features CSV files")
    val outputOpt = Opts.option[String]("output", help = "URI of the output features CSV files")
    val limitOpt = Opts.option[Int]("limit", help = "Limit number of records processed")

    val logger = Logger.getLogger(getClass)

    (featuresOpt, outputOpt).mapN { (featuresUrl, outputUrl) =>
      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Building Footprint Elevation").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate

      /* Use spark DataFrame API to read input CSV file
       * https://github.com/databricks/spark-csv
       */
      var features: DataFrame = spark.read.
        options(Map("header" -> "true", "delimiter" -> ",")).
        csv(path = featuresUrl)

      // If limit is defined apply it on input.
      limitOpt.map { n: Int => features = features.limit(n) }

      /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
      val featureRDD: RDD[Feature[Polygon, FeatureId]] =
        features.rdd.map { row: Row =>
          val state: String = row.getString(0)
          val id: Int = row.getString(1).toInt
          val wkt: String = row.getString(2)

          val geom: Polygon = WKT.read(wkt).asInstanceOf[Polygon]
          Feature(geom, FeatureId(state, id))
        }

      /* Increasing number of partitions produces better work distribution and increases reliability */
      val partitioner = new HashPartitioner(partitions = featureRDD.getNumPartitions * 16)

      val featureWithSummaryRDD: RDD[Feature[Polygon, FeatureProperties]] =
        BuildingElevation(featureRDD, partitioner)

      import spark.implicits._
      val outputDataFrame: DataFrame =
        featureWithSummaryRDD.map { feature =>
          val id = feature.data.featureId
          val (min: Double, max: Double) = feature.data.histogram.
            flatMap(_.minMaxValues()).
            getOrElse((Double.NaN, Double.NaN))

          (id.state, id.index, min, max, feature.geom.toWKT)
        }.toDF("state", "index", "min", "max", "polygon")

      outputDataFrame.write.
        options(Map("header" -> "true", "delimiter" -> ",")).
        csv(path = outputUrl)

      spark.stop
    }
  }
)
