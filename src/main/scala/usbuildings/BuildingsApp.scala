package usbuildings

import java.net.URL

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vectortile.VectorTile
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

class BuildingsApp(
  val buildingsUri: Seq[String]
)(@transient implicit val sc: SparkContext) extends LazyLogging with Serializable {
  /** Explode each layer so we can parallelize reading over layers */

  val allBuildings: RDD[Building] =
    sc.parallelize(buildingsUri, buildingsUri.length)
      .flatMap { url =>
        Building.readFromGeoJson(new URL(url))
      }

  val partitioner = new HashPartitioner(partitions=allBuildings.getNumPartitions * 16)

  // Split building geometries over skadi grid, each partition will intersect with N tiles
  val keyedBuildings: RDD[(SpatialKey, Building)] =
    allBuildings.flatMap { building =>
      val keys: Set[SpatialKey] = Terrain.terrainTilesSkadiGrid.mapTransform.keysForGeometry(building.footprint)
      // we may end up duplicating building footprint in memory if it intersects grid boundaries
      keys.map( key => (key, building) )
    }.partitionBy(partitioner)

  // per partition: set of tiles is << set of geometries

  // TODO why can't I just do polygonal summary over RasterSource ?
  val taggedBuildings: RDD[((String, Int), Building)] =
    keyedBuildings.mapPartitions { buildingsPartition =>
      val histPerTile =
        for {
          (tileKey, buildings) <- buildingsPartition.toSeq.groupBy(_._1)
          rasterSource = Terrain.getRasterSource(tileKey)
          (_, building) <- buildings
          raster <- rasterSource.read(building.footprint.envelope)
        } yield {
          val hist = raster.tile.polygonalHistogramDouble(raster.extent, building.footprint)(0)
          (building.id, building.withHistogram(hist))
        }

      histPerTile.groupBy(_._1).mapValues(_.values.reduce(_ mergeHistograms  _)).toIterator
    }.reduceByKey { (b1, b2) => b1.mergeHistograms(b2) } // Reduce results per building, now with network shuffle


  // TODO: assign buildings to keys at Zoom=X, group buildings by key
  val layoutScheme = ZoomedLayoutScheme(WebMercator)
  val layout = layoutScheme.levelForZoom(15).layout

  // I'm reprojecting them so I can make WebMercator vector tiles
  val buildingsPerWmTile: RDD[(SpatialKey, Iterable[Building])] =
    taggedBuildings.flatMap { case (id, building) =>
      val wmFootprint = building.footprint.reproject(LatLng, WebMercator)
      val reprojected = building.withFootprint(wmFootprint)
      val layoutKeys: Set[SpatialKey] = layout.mapTransform.keysForGeometry(building.footprint)
      layoutKeys.toSeq.map( key => (key, reprojected))
    }.groupByKey(partitioner)

  val tiles: RDD[(SpatialKey, VectorTile)] =
    buildingsPerWmTile.map { case (key, buildings) =>
      val extent = layout.mapTransform.keyToExtent(key)
      (key, Util.makeVectorTile(extent, buildings))
    }


}
