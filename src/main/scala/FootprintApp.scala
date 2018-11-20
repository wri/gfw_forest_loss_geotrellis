package geotrellis.demo.footprint

import java.io.File
import java.net.URI
import java.nio.file.Files

import cats.implicits._
import com.amazonaws.services.s3.AmazonS3URI
import com.monovore.decline._
import geotrellis.contrib.vlm.{RasterRef, SpatialPartitioner}
import geotrellis.vector.io.wkt.WKT
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.spark.SpatialKey
import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.util.LazyLogging
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer

import scala.util.control.NonFatal


object FootprintApp extends CommandApp(
  name = "building-footprint",
  header = "Collect building footprint elevations from DEM",
  main = {
    val buildingsOpt = Opts.option[String]("buildings", help = "URI of the building shapes layers (/, /vsis3)")
    val nedOpt = Opts.option[String]("ned", help = "URI prefix of the NED rasters (/, /vsis3")
    val outputOpt = Opts.option[String]("output", help = "URI prefix of output shapefiles (s3://, file:/)")
    val layersOpt = Opts.options[String]("layer", help = "Layer to read exclusively, empty for all layers").orEmpty

    (buildingsOpt, nedOpt, outputOpt, layersOpt).mapN { (buildingsUri, nedUri, outputUri, layers) =>
      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Building Footprint Elevation").
        set("spark.serializer", classOf[KryoSerializer].getName).
        set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

      implicit val sc: SparkContext = new SparkContext(conf)
      val app = new FootprintApp(buildingsUri, nedUri, layers)

      val outputToS3: Boolean = outputUri.startsWith("s3://")

      app.result
        .mapPartitionsWithIndex { (idx, it) => Iterator((idx, it)) }
        .foreach { case (idx, it) =>
          val shpUrl = if (outputToS3) {
            Files.createTempDirectory("footprints").toUri.toString
          } else outputUri
          val name = s"building-$idx"

          Building.writeToShapefile(s"$shpUrl/$name.shp", it)

          if (outputToS3) {
            for (ext <- List("shp", "shx", "dbf", "fix", "prj"))
              Util.uploadFile(
                new File(new URI(s"$shpUrl/$name.$ext")),
                new AmazonS3URI(s"$outputUri/$name.$ext"))
          }
      }
    }
  }
)

class FootprintApp(
  val buildingsUri: String,
  val nedUri: String,
  val layers: List[String] = List.empty
)(@transient implicit val sc: SparkContext) extends LazyLogging with Serializable {
  /** Explode each layer so we can parallelize reading over layers */
  val allBuildings: RDD[Building] = Building.getRDD(buildingsUri, layers)

  // Split building geometries over an arbitrary grid so we can parallelize our computation over this grid
  val keyedBuildings: RDD[(SpatialKey, Building)] =
    allBuildings.flatMap { building =>
      val keys: Set[SpatialKey] = NED.layoutDefinition.mapTransform.keysForGeometry(building.footprint)
      // we may end up duplicating building footprint in memory if it intersects grid boundaries
      keys.map( key => (key, building) )
    }.partitionBy(new HashPartitioner(partitions=allBuildings.getNumPartitions * 16))

  // Generate raster window references that confirm to the grid
  val keyedRasters: RDD[(SpatialKey, RasterRef)] = {
    val sources = NED.getRasters(nedUri)
      .map(_.tileToLayout(NED.layoutDefinition))
    sc.parallelize(sources, sources.length)
      .flatMap(_.toRasterRefs)
      .partitionBy(new HashPartitioner(partitions=sources.length * 16))
  }

  val joined: RDD[(SpatialKey, (Iterable[Building], Iterable[RasterRef]))] =
    keyedBuildings.cogroup(keyedRasters)
      .filter { case (key, (buildings, rasters)) =>
        // forcing an inner join here, making it easier to work with and verify subsets of the data
        rasters.nonEmpty && buildings.nonEmpty
      }

  val resultPerTile: RDD[(SpatialKey, Map[Building, Elevation])] =
    joined.mapValues { case (buildings, rasters) =>
      // perform per grid-cell cartesian join to get geometric intersection
      Util.joinBuildingsToRasters(buildings, rasters)
        .map { case (building, intersectingRasters) =>
          if (building.footprint.isValid) {
            val result: Elevation =
              intersectingRasters.map { ref: RasterRef =>
                // actually fetch rasters cells here, could inspect the source choose what or when to fetch
                val raster: Raster[MultibandTile] = ref.raster.get
                // grab histogram for band 0, assume we're working with singleband rasters
                try {
                  val hist = raster.tile.polygonalHistogramDouble(raster.extent, building.footprint).apply(0)
                  Elevation(hist.minValue.getOrElse(Double.NaN), hist.maxValue.getOrElse(Double.NaN))
                } catch {
                  case NonFatal(e) =>
                    logger.error(s"${e.getMessage} for ${building.uuid} ${WKT.write(building.footprint)} ${raster.extent}")
                    Elevation(Double.NaN, Double.NaN)
                }
              }.reduce(_ combine _) // reduce, per grid-cell, Result if we overlapped multiple NED chips
            (building, result)
          } else {
            // intersection on invalid geometries will produce non-noded intersection error, log and skip them
            logger.error(s"Invalid Geometry on ${building.uuid}: ${WKT.write(building.footprint)}")
            (building, Elevation(Double.NaN, Double.NaN))
          }
        }
    }

  val result: RDD[(Building, Elevation)] =
    resultPerTile
      .flatMap { case (_, results) => results }   // Drop grid keys and key by building
      .reduceByKey { (r1, r2) => r1.combine(r2) } // Reduce results per building, now with network shuffle
      // Final .reduceByKey introduces hash partitioning removing any spatial locality per partition
}
