package usbuildings

import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.RasterSource
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.spark.SpatialKey
import geotrellis.vector.io._
import geotrellis.vector.{Feature, Polygon}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

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
      }.partitionBy(partitioner)

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

        groupedByKey.toIterator.flatMap { case (tileKey, features) =>
          val rasterSource: RasterSource = NED.getRasterSource(tileKey)
          logger.info(s"Loading ${rasterSource.uri} for $tileKey")

          features.map { case (_, feature) =>
            // Result is optional because we may have read a non-intersecting extent
            logger.trace(s"Reading ${feature.data}}")
            val maybeRaster: Option[Raster[MultibandTile]] = rasterSource.read(feature.envelope)

            require(rasterSource.extent.intersects(feature.envelope), {
              val tileWKT = tileKey.extent(NED.tileGrid).toPolygon().toWKT
              val wktSource = rasterSource.extent.toPolygon().toWKT
              s"$tileKey: $tileWKT does not intersect RasterSource(${rasterSource.uri}): $wktSource"
            })

            val id: FeatureId = feature.data

            maybeRaster match {
              case Some(raster) =>
                val firstBandRaster: Raster[Tile] = raster.mapTile(_.band(0))

                // these imports are needed for .polygonalSummary call (to be hidden up)
                import geotrellis.contrib.polygonal.PolygonalSummary.ops._
                import geotrellis.contrib.polygonal.Implicits._
                import usbuildings.Implicits._

                val Feature(geom, histogram) =
                  firstBandRaster.polygonalSummary[Polygon, StreamingHistogram](feature.geom)

                (id, Feature(geom, FeatureProperties(id, Some(histogram))))

              case None =>
                (id, Feature(feature.geom, FeatureProperties(id, None)))
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
