package org.globalforestwatch.treecoverloss

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features.GADMFeatureId

object TreeLossRDD extends LazyLogging {

  /** Produce RDD of tree cover loss from RDD of areas of interest*
    *
    * @param featureRDD areas of interest
    * @param windowLayout window layout used for distribution of IO, subdivision of 10x10 degree grid
    * @param partitioner how to partition keys from the windowLayout
    */
  def apply(featureRDD: RDD[Feature[Geometry, Int]],
            windowLayout: LayoutDefinition,
            partitioner: Partitioner): RDD[(Int, TreeLossSummary)] = {
    /* Intersect features with each tile from windowLayout grid and generate a record for each intersection.
     * Each features will intersect one or more windows, possibly creating a duplicate record.
     * Later we will calculate partial result for each intersection and merge them.
     */
    val keyedFeatureRDD: RDD[(SpatialKey, Feature[Geometry, Int])] =
      featureRDD
        .flatMap { feature: Feature[Geometry, Int] =>
          val keys: Set[SpatialKey] =
            windowLayout.mapTransform.keysForGeometry(feature.geom)
          keys.toSeq.map { key =>
            (key, feature)
          }
        }
        .partitionBy(partitioner)

    /* Here we're going to work with the features one partition at a time.
     * We're going to use the tile key from windowLayout to read pixels from appropriate raster.
     * Each record in this RDD may still represent only a partial result for that feature.
     *
     * The RDD is keyed by Id such that we can join and recombine partial results later.
     */
    val featuresWithSummaries: RDD[(Int, TreeLossSummary)] =
      keyedFeatureRDD.mapPartitions {
        featurePartition: Iterator[
          (SpatialKey, Feature[Geometry, Int])
          ] =>
          // Code inside .mapPartitions works in an Iterator of records
          // Doing things this way allows us to reuse resources and perform other optimizations

          // Grouping by spatial key allows us to minimize read thrashing from record to record

          val groupedByKey
          : Map[SpatialKey,
            Array[(SpatialKey, Feature[Geometry, Int])]] =
            featurePartition.toArray.groupBy {
              case (windowKey, feature) => windowKey
            }

          groupedByKey.toIterator.flatMap {
            case (windowKey, keysAndFeatures) =>
              // round to one integer to assure we have 400 * 400 blocks
              val window: Extent = windowKey.extent(windowLayout)

              //              val xmin: Double = _window.xmin
              //              val ymin: Double = _window.ymin
              //              val xmax: Double = _window.xmax
              //              val ymax: Double = _window.ymax
              //
              //              val window = new RoundedExtent(xmin, ymin, xmax, ymax, 1)

              val maybeRasterSource: Either[Throwable, TreeLossGridSources] =
                Either.catchNonFatal {
                  TreeLossGrid.getRasterSource(window)
                }

              val features = keysAndFeatures.map(_._2)

              val maybeRaster: Either[Throwable, Raster[TreeLossTile]] =
                maybeRasterSource.flatMap { rs: TreeLossGridSources =>
                  rs.readWindow(window)
                }

              // flatMap here flattens out and ignores the errors
              features.flatMap { feature: Feature[Geometry, Int] =>
                val id: Int = feature.data
                val rasterizeOptions = Rasterizer.Options(
                  includePartial = false,
                  sampleType = PixelIsPoint
                )

                maybeRaster match {
                  case Left(exception) =>
                    logger.error(s"Feature $id: $exception")
                    List.empty

                  case Right(raster) =>
                    val summary: TreeLossSummary =
                      try {
                        raster.polygonalSummary(
                          geometry = feature.geom,
                          emptyResult = new TreeLossSummary(),
                          options = rasterizeOptions
                        )
                      } catch {
                        case ise: java.lang.IllegalStateException => {
                          println(s"There is an issue with geometry for ${feature.data}")
                          throw ise
                        }
                        case te: org.locationtech.jts.geom.TopologyException => {
                          println(s"There is an issue with geometry Topology for ${feature.data}")
                          throw te
                        }
                        case e: Throwable => throw e

                      }
                    List((id, summary))
                }
              }
          }
      }

    /* Group records by Id and combine their summaries
     * The features may have intersected multiple grid blocks
     */
    val featuresGroupedWithSummaries: RDD[(Int, TreeLossSummary)] =
      featuresWithSummaries.reduceByKey {
        case (summary1, summary2) =>
          summary1.merge(summary2)
      }

    featuresGroupedWithSummaries
  }
}
