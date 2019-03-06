package usbuildings

import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.RasterSource
import geotrellis.raster.{MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.spark.SpatialKey
import geotrellis.spark.io.index.zcurve.Z2
import geotrellis.vector.io._
import geotrellis.vector.{Feature, Point, Polygon}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import cats.implicits._

object BuildingElevation extends LazyLogging {

  def apply(
    featureRDD: RDD[Feature[Polygon, FeatureId]],
    partitioner: Partitioner
  ): RDD[Feature[Polygon, FeatureProperties]] = {
    /* Intersect features with NED tile grid and generate a record for each intersection.
    * When a feature is on tile edges it may intersect multiple tiles.
    * Later we will calculate partial result for each intersection and merge them.
    */
    val keyedFeatureRDD: RDD[(SpatialKey, Feature[Polygon, FeatureId])] =
      featureRDD.flatMap { feature =>
        // set of tile grid keys that intersect this particular feature
        val keys: Set[SpatialKey] = NED.tileGrid.mapTransform.keysForGeometry(feature.geom)
        keys.map { key => (key, feature) }
      }.partitionBy(partitioner) // this will produce stragler partitions for us, lets talk about why

    /* Here we're going to work with the features one partition at a time.
     * We're going to use the tile grid key to read pixels from appropriate raster.
     * Each record in this RDD may still represent only a partial result for that feature.
     *
     * The RDD is keyed by FeatureId such that we can join and recombine partial results later.
     */
    val featuresWithSummaries: RDD[(FeatureId, Feature[Polygon, FeatureProperties])] =
      keyedFeatureRDD.mapPartitions { featurePartition =>
        // Code inside .mapPartitions works in an Iterator of records
        // Doing things this way allows us to reuse resources and perform other optimizations

        // Grouping by spatial key allows us to minimize reading thrashing from record to record
        val groupedByKey: Map[SpatialKey, Array[(SpatialKey,Feature[Polygon, FeatureId])]] =
          featurePartition.toArray.groupBy(_._1)

        groupedByKey.toIterator.flatMap { case (tileKey, featuresAndKeys) =>
          val maybeRasterSource: Either[Throwable, RasterSource] = Either.catchNonFatal {
            val rs = NED.getRasterSource(tileKey)
            logger.info(s"Load ${rs.uri} for $tileKey")
            rs
          }

          val features = featuresAndKeys.map(_._2)
          val tileRasterExtent: RasterExtent = NED.tileRasterExtent(tileKey)

          Util.sortByZIndex(features, tileRasterExtent).map { feature =>
            logger.trace(s"Reading ${feature.data}}")

            val maybeRaster: Either[Throwable, Option[Raster[MultibandTile]]] =
              maybeRasterSource.flatMap { rs => Either.catchNonFatal(rs.read(feature.envelope)) }

            val id: FeatureId = feature.data

            maybeRaster match {
              case Left(exception) =>
                logger.error(s"Feature $id: $exception")
                (id, Feature(feature.geom, FeatureProperties(id, None)))

              case Right(None) =>
                logger.warn(s"Feature $id: did not intersect RasterSource")
                (id, Feature(feature.geom, FeatureProperties(id, None)))

              case Right(Some(raster)) =>
                val firstBandRaster: Raster[Tile] = raster.mapTile(_.band(0))

                // these imports are needed for .polygonalSummary call (to be hidden up)
                import geotrellis.contrib.polygonal.PolygonalSummary.ops._
                import geotrellis.contrib.polygonal.Implicits._
                import usbuildings.Implicits._

                val Feature(geom, histogram) =
                  firstBandRaster.polygonalSummary[Polygon, StreamingHistogram](feature.geom)

                (id, Feature(geom, FeatureProperties(id, Some(histogram))))
              }
          }
        }
      }

    /* Group records by FeatureId and combine their summaries.
     * This RDD
     */
    val featuresGroupedWithSummaries: RDD[(FeatureId, Feature[Polygon, FeatureProperties])] =
      featuresWithSummaries.reduceByKey { case (Feature(geom, payload1), Feature(_, payload2)) =>
        Feature(geom, payload1.merge(payload2))
      }

    // discard FeatureId key after the aggregation
    featuresGroupedWithSummaries.values
  }
}
