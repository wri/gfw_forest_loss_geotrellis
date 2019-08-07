package org.globalforestwatch.util

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.grids.GridSources
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

trait AnalysisRDD extends LazyLogging with java.io.Serializable {

  type SOURCES <: GridSources
  //  type FEATUREID <: FeatureId
  type SUMMARY <: Summary[SUMMARY]
  type TILE <: CellGrid

  /** Produce RDD of tree cover loss from RDD of areas of interest*
    *
    * @param featureRDD areas of interest
    * @param windowLayout window layout used for distribution of IO, subdivision of 10x10 degree grid
    * @param partitioner how to partition keys from the windowLayout
    */
  def apply[FEATUREID <: FeatureId](
                                     featureRDD: RDD[Feature[Geometry, FEATUREID]],
                                     windowLayout: LayoutDefinition,
                                     partitioner: Partitioner,
                                     tcdYear: Int = 2000)(implicit kt: ClassTag[SUMMARY], vt: ClassTag[FEATUREID], ord: Ordering[SUMMARY] = null): RDD[(FEATUREID, SUMMARY)] = {

    /* Intersect features with each tile from windowLayout grid and generate a record for each intersection.
     * Each features will intersect one or more windows, possibly creating a duplicate record.
     * Later we will calculate partial result for each intersection and merge them.
     */
    val keyedFeatureRDD: RDD[(SpatialKey, Feature[Geometry, FEATUREID])] =
      featureRDD
        .flatMap { feature: Feature[Geometry, FEATUREID] =>
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
    val featuresWithSummaries: RDD[(FEATUREID, SUMMARY)] =
      keyedFeatureRDD.mapPartitions {
        featurePartition: Iterator[
          (SpatialKey, Feature[Geometry, FEATUREID])
        ] =>
          // Code inside .mapPartitions works in an Iterator of records
          // Doing things this way allows us to reuse resources and perform other optimizations

          // Grouping by spatial key allows us to minimize read thrashing from record to record

          val groupedByKey
            : Map[SpatialKey,
                  Array[(SpatialKey, Feature[Geometry, FEATUREID])]] =
            featurePartition.toArray.groupBy {
              case (windowKey, feature) => windowKey
            }

          groupedByKey.toIterator.flatMap {
            case (windowKey, keysAndFeatures) =>
              val window: Extent = windowKey.extent(windowLayout)
              val maybeRasterSource: Either[Throwable, SOURCES] =
                getSources(window)

              val features = keysAndFeatures.map(_._2)

              val maybeRaster: Either[Throwable, Raster[TILE]] =
                maybeRasterSource.flatMap { rs: SOURCES =>
                  readWindow(rs, window)
                }

              // flatMap here flattens out and ignores the errors
              features.flatMap { feature: Feature[Geometry, FEATUREID] =>
                val id: FEATUREID = feature.data
                val rasterizeOptions = Rasterizer.Options(
                  includePartial = false,
                  sampleType = PixelIsPoint
                )

                maybeRaster match {
                  case Left(exception) =>
                    logger.error(s"Feature $id: $exception")
                    List.empty

                  case Right(raster) =>
                    val summary: SUMMARY =
                      try {
                        runPolygonalSummary(
                          raster,
                          feature.geom,
                          rasterizeOptions,
                          tcdYear
                        )

                      } catch {
                        case ise: java.lang.IllegalStateException => {
                          println(
                            s"There is an issue with geometry for ${feature.data}"
                          )
                          throw ise
                        }
                        case te: org.locationtech.jts.geom.TopologyException => {
                          println(
                            s"There is an issue with geometry Topology for ${feature.data}"
                          )
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
    val featuresGroupedWithSummaries: RDD[(FEATUREID, SUMMARY)] =
      reduceSummarybyKey[FEATUREID](featuresWithSummaries): RDD[(FEATUREID, SUMMARY)]
    featuresGroupedWithSummaries
  }

  def getSources(window: Extent): Either[Throwable, SOURCES]

  def readWindow(rs: SOURCES, window: Extent): Either[Throwable, Raster[TILE]]

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          tcdYear: Int): SUMMARY

  def reduceSummarybyKey[FEATUREID <: FeatureId](
    featuresWithSummaries: RDD[(FEATUREID, SUMMARY)]
                                                )(implicit kt: ClassTag[SUMMARY], vt: ClassTag[FEATUREID], ord: Ordering[SUMMARY] = null): RDD[(FEATUREID, SUMMARY)] = {
    featuresWithSummaries.reduceByKey {
      case (summary1, summary2) =>
        summary1.merge(summary2)
    }

  }
}
