package usbuildings

import org.locationtech.geomesa.spark.jts._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import com.monovore.decline.{CommandApp, Opts}
import cats.implicits._
import java.net.URL

import cats.Monoid
import geotrellis.contrib.polygonal.CellAccumulator
import geotrellis.contrib.vlm.RasterSource
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.raster.histogram.{Histogram, StreamingHistogram}
import geotrellis.spark.SpatialKey
import geotrellis.vector.{Feature, Polygon}
import org.apache.spark.rdd.RDD

import scala.util.Try

object Main extends CommandApp(
  name = "geotrellis-usbuildings",
  header = "Collect building footprint elevations from terrain tiles",
  main = {
    val featuresOpt = Opts.option[String]("features", help = "URI of the features CSV file")

    (featuresOpt).map { (featuresUrl) =>
      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Building Footprint Elevation").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate

      val features: DataFrame = spark.read.
        options(Map("header" -> "true", "delimiter" -> ",")).
        csv(path = featuresUrl)

      val featureRDD: RDD[Feature[Polygon, FeatureId]] = ???

      val partitioner = new HashPartitioner(partitions = featureRDD.getNumPartitions * 16)

      // Split building geometries over Skadi grid, each partition will intersect with N tiles
      val keyedFeatureRDD: RDD[(SpatialKey, Feature[Polygon, FeatureId])] =
        featureRDD.flatMap { feature =>
          /* TODO: DOC Tile LayoutDefinition can be adjusted */
          val keys: Set[SpatialKey] = Terrain.terrainTilesSkadiGrid.mapTransform.keysForGeometry(feature.geom)
          keys.map { key => (key, feature) }
        }.partitionBy(partitioner)

      val featuresWithSummaries: RDD[(FeatureId, Feature[Polygon, StreamingHistogram])] =
        keyedFeatureRDD.mapPartitions { featurePartition =>

          val groupedByKey = featurePartition.toArray.groupBy(_._1)
          groupedByKey.toIterator.flatMap { case (tileKey, features) =>
            val rasterSource: RasterSource = Terrain.getRasterSource(tileKey)

            features.map { case (_, feature) =>
              val id: FeatureId = feature.data
              val maybeRaster: Option[Raster[MultibandTile]] = rasterSource.read(feature.envelope)

              // TODO: DOC Training material for Monoid
              import geotrellis.contrib.polygonal.PolygonalSummary.ops._
              import geotrellis.contrib.polygonal.Implicits._
              import usbuildings.Implicits._

              maybeRaster match {
                case Some(raster) =>
                  val firstBandRaster: Raster[Tile] = raster.mapTile(_.band(0))
                  val newFeature = firstBandRaster.polygonalSummary[Polygon, StreamingHistogram](feature.geom)
                  (id, newFeature)
                case None =>
                  val newFeature = feature.mapData( _ => Monoid[StreamingHistogram].empty)
                  (id, newFeature)
              }
            }
          }
        }

      val featuresGroupedWithSummaries: RDD[(FeatureId, Feature[Polygon, StreamingHistogram])] =
      featuresWithSummaries.reduceByKey { (feature1, feature2) =>
        ???
      }


      // Reduce results per building, now with network shuffle

      // What I don't like about this:
      // [x] ... I got the geometries.
      // I don't believe that current method scales well
      // ... it relies on per JVM GDAL cache
      // ... It is not easy to bring in tiles on different grids
      // !!! actually cache is the largest problem
      // TODO: rename buildings to Features, work with Feature[Polygon, Data]
      // TODO: add comments
      // TODO: Add tests that explain what is going on

      // TODO: Decode features to RDD[Feature[Polygon, FeatureId]]


      features

      spark.stop

      // TODO: add utility to list files from path when not given explicitly
    }
  }
)
