package usbuildings

import java.net.URL

import com.monovore.decline.{CommandApp, Opts}
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import cats.implicits._

object Main extends CommandApp(
  name = "geotrellis-usbuildings",
  header = "Collect building footprint elevations from terrain tiles",
  main = {
    val buildingsAllOpt = Opts.flag(long = "all-buildings", help = "Read all building footprints from hosted source.").orFalse
    val buildingsOpt = Opts.option[String]("buildings", help = "URI of the building shapes layers")
    val outputOpt = Opts.option[String]("output", help = "URI prefix of output shapefiles")
    val layersOpt = Opts.options[String]("layer", help = "Layer to read exclusively, empty for all layers").orEmpty

    (buildingsAllOpt, buildingsOpt, outputOpt, layersOpt).mapN { (buildingsAll, buildingsUri, outputUri, layers) =>
      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Building Footprint Elevation").
        set("spark.serializer", classOf[KryoSerializer].getName).
        set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate
      implicit val sc: SparkContext = ss.sparkContext

      val app = if (buildingsAll) {
        new BuildingsApp(Building.geoJsonURLs)
      } else {
        new BuildingsApp(List(buildingsUri))
      }

      // TODO: use outputUri
      GenerateVT.save(app.tiles, zoom= 15, "geotrellis-test", "usbuildings/vt02")

    }
  }
)
