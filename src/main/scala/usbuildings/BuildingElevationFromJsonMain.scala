package usbuildings

import java.net.URL

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.io._
import geotrellis.vector.io.wkt.WKT
import geotrellis.vector.{Feature, Polygon}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf}

object BuildingElevationFromJsonMain extends CommandApp (
  name = "geotrellis-usbuildings",
  header = "Collect building footprint elevations from terrain tiles",
  main = {
    val allFeaturesOpt = Opts.flag("all-features", "Use input from all available states")
    val featuresOpt = Opts.options[String]("features", "URI of building GeoJSON file (optionally zipped)")
    val outputOpt = Opts.option[String]("output", help = "URI of the output features CSV files")
    val sampleOpt = Opts.option[Double]("sample", help = "Fraction of input to sample").orNone

    val logger = Logger.getLogger(getClass)

    val featuresListOpt: Opts[List[String]] =
      allFeaturesOpt.map( _ => Building.geoJsonURLs ).
      orElse(featuresOpt.map(_.toList))

    (featuresListOpt, outputOpt, sampleOpt).mapN { (featuresUrl, outputUrl, sample) =>
      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Building Footprint Elevation").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate

      /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
      var featureRDD: RDD[Feature[Polygon, FeatureId]] =
        spark.sparkContext.
          parallelize(featuresUrl, featuresUrl.length).
          flatMap { url =>
            Building.readFromGeoJson(new URL(url))
          }

      sample.map { r =>
        logger.info(s"Taking sample of ${r * 100} %")
        featureRDD = featureRDD.sample(withReplacement = false, fraction = r, seed = 0L)
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
