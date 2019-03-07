package usbuildings

import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.RasterSource
import geotrellis.raster.{MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.spark.SpatialKey
import geotrellis.spark.io.index.zcurve.Z2
import geotrellis.vector.io._
import geotrellis.raster._
import geotrellis.vector._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import cats.implicits._
import geotrellis.contrib.polygonal._
import Implicits._
import org.apache.spark.sql.{DataFrame, SparkSession}


object TreeLossJob extends LazyLogging {

  def apply(
    featureRDD: RDD[Feature[Geometry, Id]],
    partitioner: Partitioner
  ): RDD[(Id, TreeLossSummary)] = {
    /* Intersect features with NED tile grid and generate a record for each intersection.
    * When a feature is on tile edges it may intersect multiple tiles.
    * Later we will calculate partial result for each intersection and merge them.
    */
    val keyedFeatureRDD: RDD[(SpatialKey, Feature[Geometry, Id])] =
      featureRDD.flatMap { feature: Feature[Geometry, Id] =>
        // set of tile grid keys that intersect this particular feature
        val keys: Set[SpatialKey] = RasterUtils.tileGrid.mapTransform.keysForGeometry(feature.geom)
        keys.toSeq.map { key => (key, feature) }
      }.partitionBy(partitioner)

    /* Here we're going to work with the features one partition at a time.
     * We're going to use the tile grid key to read pixels from appropriate raster.
     * Each record in this RDD may still represent only a partial result for that feature.
     *
     * The RDD is keyed by FeatureId such that we can join and recombine partial results later.
     */

//// We could do this instead of .mapPartitions
//    val res: RDD[(Id, Feature[Geometry, TreeLossRecord])] =
//    keyedFeatureRDD.map { case (key, feature) =>
//      val rasterSource: RasterSource = ??? // use the key to figure out the URI and open a reader for it
//      val raster = rasterSource.read(feature.geom.envelope).get
//      val summary = raster.polygonalSummary(feature.geom, TreeLossSummary())
//      val id = feature.data
//      (id, Feature(feature.geom, TreeLossRecord(id, summary)))
//    }
//
    val featuresWithSummaries: RDD[(Id, TreeLossSummary)] =
      keyedFeatureRDD.mapPartitions { featurePartition =>
        // Code inside .mapPartitions works in an Iterator of records
        // Doing things this way allows us to reuse resources and perform other optimizations

        // Grouping by spatial key allows us to minimize reading thrashing from record to record
        val groupedByKey: Map[SpatialKey, Array[(SpatialKey, Feature[Geometry, Id])]] =
          featurePartition.toArray.groupBy(_._1)

        groupedByKey.toIterator.flatMap { case (tileKey, featuresAndKeys) =>
          val maybeRasterSource: Either[Throwable, TenByTenGridSources] =
            Either.catchNonFatal { RasterUtils.getRasterSource(tileKey) }

          val features = featuresAndKeys.map(_._2)
          val tileExtent: Extent = tileKey.extent(RasterUtils.tileGrid)

          val maybeRaster: Either[Throwable, Raster[TreeLossTile]] =
            maybeRasterSource.flatMap { rs: TenByTenGridSources =>
              Either.catchNonFatal {

                // TODO: Talk about how to make biomass optional and still produce an answer
                logger.info(s"Reading: $tileKey, ${rs.forestChangeSourceUri}")
                val lossYear: MultibandTile = rs.forestChangeSource.read(tileExtent).get.tile.withNoData(Some(0))
//                logger.info(s"YearLoss: ${lossYear.band(0).findMinMaxDouble}")

                logger.info(s"Reading: $tileKey, ${rs.treeCoverSourceUri}")
                val treeCover: MultibandTile = rs.treeCoverSource.read(tileExtent).get.tile
//                logger.info(s"treeCover: ${treeCover.band(0).findMinMaxDouble}")

                logger.info(s"Reading: $tileKey, ${rs.bioMassSourceUri}")
                val biomass: MultibandTile = rs.bioMassSource.read(tileExtent).get.tile
//                logger.info(s"biosmass: ${biomass.band(0).findMinMaxDouble}")

                val tile = TreeLossTile(
                  lossYear.band(0),
                  treeCover.band(0),
                  biomass.band(0))
                Raster(tile, tileExtent)
              }
            }

          features.flatMap { feature: Feature[Geometry, Id] =>
            logger.trace(s"Reading ${feature.data}}")

            val id: Id = feature.data
            val geom: Geometry = feature.geom

            maybeRaster match {
              case Left(exception) =>
                logger.error(s"Feature $id: $exception")
                List.empty

              case Right(raster) =>
                val summary = raster.polygonalSummary[TreeLossSummary](feature.geom, TreeLossSummary())
                List((id, summary))
            }
          }
        }
      }

    /* Group records by FeatureId and combine their summaries.
     * This RDD
     */
    val featuresGroupedWithSummaries: RDD[(Id, TreeLossSummary)] =
      featuresWithSummaries.reduceByKey { case (summary1, summary2) =>
        summary1.merge(summary2)
      }

    // discard FeatureId key after the aggregation
    featuresGroupedWithSummaries
  }
}
