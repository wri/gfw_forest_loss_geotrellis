package usbuildings

import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.io._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import cats._
import cats.implicits._

/** Spark Job to translate GeoJSON feature collections to CSV */
object GeoJsonToCsvMain extends CommandApp (
  name = "geojson-to-csv",
  header = "Convert GeoJSON feature collections to CSV with WKT fields",
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
    }
  }
)