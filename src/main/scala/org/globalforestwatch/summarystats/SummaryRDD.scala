package org.globalforestwatch.summarystats

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.summary.polygonal.{NoIntersection, PolygonalSummaryResult}
import geotrellis.raster.summary.polygonal
import geotrellis.store.index.zcurve.Z2
import geotrellis.vector.Extent.toPolygon
import geotrellis.vector._
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.grids.GridSources
import org.globalforestwatch.util.RepartitionSkewedRDD

import scala.reflect.ClassTag


trait SummaryRDD extends LazyLogging with java.io.Serializable {

  type SOURCES <: GridSources
  type SUMMARY <: Summary[SUMMARY]
  type TILE <: CellGrid[Int]

  /** Produce RDD of tree cover loss from RDD of areas of interest*
    *
    * @param featureRDD areas of interest
    * @param windowLayout window layout used for distribution of IO, subdivision of 10x10 degree grid
    * @param partitionType flag of whether to partition RDD while processing
    */
  def apply[FEATUREID <: FeatureId](
                                     featureRDD: RDD[Feature[Geometry, FEATUREID]],
                                     windowLayout: LayoutDefinition,
                                     kwargs: Map[String, Any],
                                     partitionType: String = "SKEWED")(implicit kt: ClassTag[SUMMARY], vt: ClassTag[FEATUREID], ord: Ordering[SUMMARY] = null): RDD[(FEATUREID, SUMMARY)] = {

    /* Intersect features with each tile from windowLayout grid and generate a record for each intersection.
     * Each features will intersect one or more windows, possibly creating a duplicate record.
     * Then create a key based off the Z curve value from the grid cell, to use for partitioning.
     * Later we will calculate partial result for each intersection and merge them.
     */

    val keyedFeatureRDD: RDD[(Long, (SpatialKey, Feature[Geometry, FEATUREID]))] = featureRDD
      .flatMap { feature: Feature[Geometry, FEATUREID] =>
        val keys: Set[SpatialKey] =
          windowLayout.mapTransform.keysForGeometry(feature.geom)
        keys.toSeq.map { key =>
          val z = Z2(key.col, key.row).z
          (z, (key, feature))
        }
      }

    /*
     * Use a Range Partitioner based on the Z curve value to efficiently and evenly partition RDD for analysis,
     * but still preserving locality which will both reduce the S3 reads per executor and make it more likely
     * for features to be close together already during export.
     */

    val partitionedFeatureRDD =
      if (partitionType.equals("SKEWED")) {
        RepartitionSkewedRDD.bySparseId(keyedFeatureRDD, 4096)
      } else if (partitionType.equals("RANGE")) {
        val rangePartitioner =
          new RangePartitioner(featureRDD.sparkContext.defaultParallelism, keyedFeatureRDD)
        keyedFeatureRDD.partitionBy(rangePartitioner).values
      } else {
        keyedFeatureRDD.values
      }

    println(s"Number of partitions: ${partitionedFeatureRDD.getNumPartitions}")

    /*
     * Here we're going to work with the features one partition at a time.
     * We're going to use the tile key from windowLayout to read pixels from appropriate raster.
     * Each record in this RDD may still represent only a partial result for that feature.
     *
     * The RDD is keyed by Id such that we can join and recombine partial results later.
     */
    val featuresWithSummaries: RDD[(FEATUREID, SUMMARY)] =
      partitionedFeatureRDD.mapPartitions {
        featurePartition: Iterator[(SpatialKey, Feature[Geometry, FEATUREID])] =>
        // Code inside .mapPartitions works in an Iterator of records
          // Doing things this way allows us to reuse resources and perform other optimizations
          // Grouping by spatial key allows us to minimize read thrashing from record to record

          val windowFeature = featurePartition.map {
            case (windowKey, feature) =>
              (windowKey, feature)
          }

          val groupedByKey
          : Map[SpatialKey,
            Array[(SpatialKey, Feature[Geometry, FEATUREID])]] =
            windowFeature.toArray.groupBy {
              case (windowKey, feature) => windowKey
            }

          groupedByKey.toIterator.flatMap {
            case (windowKey, keysAndFeatures) =>
              val maybeRasterSource: Either[Throwable, SOURCES] =
                getSources(windowKey, windowLayout, kwargs)

              val features = keysAndFeatures map { case (_, feature) => feature }

              val maybeRaster: Either[Throwable, Raster[TILE]] =
                maybeRasterSource.flatMap { rs: SOURCES =>
                  readWindow(rs, windowKey, windowLayout)
                }


              // Split features into those that completely contain the current window
              // and those that only partially contain it
              val windowGeom: Extent = windowLayout.mapTransform.keyToExtent(windowKey)
              val (fullWindowFeatures, partialWindowFeatures) = features.partition {
                feature =>
                  try {
                    feature.geom.contains(windowGeom)
                  } catch {
                    case e: org.locationtech.jts.geom.TopologyException =>
                      // fallback if JTS can't do the intersection because of a wonky geometry,
                      // just skip the optimization
                      false
                  }
              }

              def getSummaryForGeom(featureIds: List[FEATUREID], geom: Geometry) = {
                val rasterizeOptions = Rasterizer.Options(
                  includePartial = false,
                  sampleType = PixelIsPoint
                )

                maybeRaster match {
                  case Left(exception) =>
                    logger.error(s"Feature(s) $featureIds: $exception")
                    List.empty

                  case Right(raster) =>
                    val summary: Option[SUMMARY] =
                      try {
                        runPolygonalSummary(
                          raster,
                          geom,
                          rasterizeOptions,
                          kwargs
                        ) match {
                          case polygonal.Summary(result: SUMMARY) => Some(result)
                          case NoIntersection => None
                        }
                      } catch {
                        case ise: java.lang.IllegalStateException => {
                          println(
                            s"There is an issue with geometry for feature(s) $featureIds"
                          )
                          // TODO some very invalid geoms are somehow getting here, skip for now
                          None
                        }
                        case te: org.locationtech.jts.geom.TopologyException => {
                          println(
                            s"There is an issue with geometry for feature(s) $featureIds: ${geom}"
                          )
                          None
                        }
                        case be: java.lang.ArrayIndexOutOfBoundsException => {
                          println(
                            s"There is an issue with geometry for feature(s) $featureIds: ${geom}"
                          )
                          None
                        }
                        case ise: java.lang.IllegalArgumentException => {
                          println(
                            s"There is an issue with geometry for feature(s) $featureIds: ${geom}"
                          )
                          None
                        }
                      }

                    summary match {
                      case Some(result) => featureIds.map { case featureId => (featureId, result) }
                      case None => List.empty
                    }
                }
              }

              // for partial windows, we need to calculate summary for each geometry,
              // since they all may have unique intersections with the window
              val partialWindowResults = partialWindowFeatures.flatMap {
                case feature =>
                  getSummaryForGeom(List(feature.data), feature.geom)
              }

              // if there are any full window intersections, we only need to calculate
              // the summary for the window, and then tie it to each feature ID
              val fullWindowIds = fullWindowFeatures.map { case feature => feature.data}.toList
              val fullWindowResults =
                if (fullWindowFeatures.nonEmpty) {
                  getSummaryForGeom(fullWindowIds, windowGeom)
                } else {
                  List.empty
                }

              // combine results
              partialWindowResults ++ fullWindowResults
          }
      }

    /* Group records by Id and combine their summaries
     * The features may have intersected multiple grid blocks
     */
    val featuresGroupedWithSummaries: RDD[(FEATUREID, SUMMARY)] =
      reduceSummarybyKey[FEATUREID](featuresWithSummaries): RDD[(FEATUREID, SUMMARY)]
    print(featuresGroupedWithSummaries.toDebugString)

    featuresGroupedWithSummaries
  }

  def getSources(key: SpatialKey, windowLayout: LayoutDefinition, kwargs: Map[String, Any]): Either[Throwable, SOURCES]

  def readWindow(rs: SOURCES, windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[TILE]]

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          kwargs: Map[String, Any]): PolygonalSummaryResult[SUMMARY]


  def reduceSummarybyKey[FEATUREID <: FeatureId](
    featuresWithSummaries: RDD[(FEATUREID, SUMMARY)]
                                                )(implicit kt: ClassTag[SUMMARY], vt: ClassTag[FEATUREID], ord: Ordering[SUMMARY] = null): RDD[(FEATUREID, SUMMARY)] = {
    featuresWithSummaries.reduceByKey {
      case (summary1, summary2) =>
        summary1.merge(summary2)
    }
  }
}
