package org.globalforestwatch.treecoverloss

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.spark.SpatialKey
import geotrellis.vector._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.spark.tiling.LayoutDefinition
import Implicits._

object TreeLossRDD extends LazyLogging {

  /** Produce RDD of tree cover loss from RDD of areas of interest*
    *
    * @param featureRDD areas of interest
    * @param windowLayout window layout used for distribution of IO, subdivision of 10x10 degree grid
    * @param partitioner how to partition keys from the windowLayout
    */
  def apply(
    featureRDD: RDD[Feature[Geometry, FeatureId]],
    windowLayout: LayoutDefinition,
    partitioner: Partitioner
  ): RDD[(FeatureId, TreeLossSummary)] = {
    /* Intersect features with each tile from windowLayout grid and generate a record for each intersection.
     * Each features will intersect one or more windows, possibly creating a duplicate record.
     * Later we will calculate partial result for each intersection and merge them.
     */
    val keyedFeatureRDD: RDD[(SpatialKey, Feature[Geometry, FeatureId])] =
      featureRDD.flatMap { feature: Feature[Geometry, FeatureId] =>
        val keys: Set[SpatialKey] = windowLayout.mapTransform.keysForGeometry(feature.geom)
        keys.toSeq.map { key => (key, feature) }
      }.partitionBy(partitioner)

    /* Here we're going to work with the features one partition at a time.
     * We're going to use the tile key from windowLayout to read pixels from appropriate raster.
     * Each record in this RDD may still represent only a partial result for that feature.
     *
     * The RDD is keyed by Id such that we can join and recombine partial results later.
     */
    val featuresWithSummaries: RDD[(FeatureId, TreeLossSummary)] =
      keyedFeatureRDD.mapPartitions { featurePartition: Iterator[(SpatialKey, Feature[Geometry, FeatureId])] =>
        // Code inside .mapPartitions works in an Iterator of records
        // Doing things this way allows us to reuse resources and perform other optimizations

        // Grouping by spatial key allows us to minimize read thrashing from record to record
        val groupedByKey: Map[SpatialKey, Array[(SpatialKey, Feature[Geometry, FeatureId])]] =
          featurePartition.toArray.groupBy{ case (windowKey, feature) => windowKey }

        groupedByKey.toIterator.flatMap { case (windowKey, keysAndFeatures) =>
          val window: Extent = windowKey.extent(windowLayout)

          val maybeRasterSource: Either[Throwable, TenByTenGridSources] =
            Either.catchNonFatal {
              TenByTenGrid.getRasterSource(window)
            }

          val features = keysAndFeatures.map(_._2)

          val maybeRaster: Either[Throwable, Raster[TreeLossTile]] =
            maybeRasterSource.flatMap { rs: TenByTenGridSources =>
              Either.catchNonFatal {
                // TODO: Talk about how to make biomass optional and still produce an answer
                logger.info(s"Reading: $windowKey, ${rs.forestChangeSourceUri}")
                val lossYear: MultibandTile = rs.forestChangeSource.read(window).get.tile.withNoData(Some(0))

                logger.info(s"Reading: $windowKey, ${rs.treeCoverSourceUri}")
                val treeCover: MultibandTile = rs.treeCoverSource.read(window).get.tile

                logger.info(s"Reading: $windowKey, ${rs.bioMassSourceUri}")
                val biomass: MultibandTile = rs.bioMassSource.read(window).get.tile

                val tile = TreeLossTile(
                  lossYear.band(0),
                  treeCover.band(0),
                  biomass.band(0))
                Raster(tile, window)
              }
            }

          // flatMap here flattens out and ignores the errors
          features.flatMap { feature: Feature[Geometry, FeatureId] =>
            val id: FeatureId = feature.data

            maybeRaster match {
              case Left(exception) =>
                logger.error(s"Feature $id: $exception")
                List.empty

              case Right(raster) =>
                val summary: TreeLossSummary =
                  raster.polygonalSummary(
                    geometry = feature.geom,
                    emptyResult = new TreeLossSummary())

                List((id, summary))
            }
          }
        }
      }

    /* Group records by Id and combine their summaries
     * The features may have intersected multiple grid blocks
     */
    val featuresGroupedWithSummaries: RDD[(FeatureId, TreeLossSummary)] =
      featuresWithSummaries.reduceByKey { case (summary1, summary2) =>
        summary1.merge(summary2)
      }

    featuresGroupedWithSummaries
  }
}
