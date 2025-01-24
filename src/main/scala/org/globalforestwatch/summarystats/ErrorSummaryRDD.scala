package org.globalforestwatch.summarystats

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.summary.polygonal.{PolygonalSummaryResult, Summary => GTSummary}
import geotrellis.store.index.zcurve.Z2
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.grids.GridSources
import org.globalforestwatch.util.RepartitionSkewedRDD

import scala.reflect.ClassTag
import cats.kernel.Semigroup
import cats.data.Validated.{Invalid, Valid}


trait ErrorSummaryRDD extends LazyLogging with java.io.Serializable {

  type SOURCES <: GridSources
  type SUMMARY <: Summary[SUMMARY]
  type TILE <: CellGrid[Int]

  /** Produce RDD of polygonal summary analysis from RDD of features.
    *
    * @param featureRDD areas of interest
    * @param windowLayout window layout used for distribution of IO, subdivision of 10x10 degree grid
    * @param partition flag of whether to partition RDD while processing
    */
  def apply[FEATUREID <: FeatureId](
    featureRDD: RDD[Feature[Geometry, FEATUREID]],
    windowLayout: LayoutDefinition,
    kwargs: Map[String, Any],
    partition: Boolean = true
  )(implicit kt: ClassTag[SUMMARY], vt: ClassTag[FEATUREID]): RDD[ValidatedLocation[SUMMARY]] = {

    /* Intersect features with each tile from windowLayout grid and generate a record for each intersection.
     * Each feature will intersect one or more windows, possibly creating multiple records.
     * Then create a key based off the Z curve value for each intersecting window, to use for partitioning.
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
     * Use a Hash Partitioner based on the Z curve value to efficiently and evenly partition RDD for analysis,
     * but still preserving locality which will both reduce the S3 reads per executor and make it more likely
     * for features to be close together already during export.
     */
    val partitionedFeatureRDD = if (partition) {
      // Generally, features or parts of features intersecting the same window will
      // go into the same partition. If a single window includes parts of more than
      // 4096 features, then those parts will be split up over multiple partitions.
      RepartitionSkewedRDD.bySparseId(keyedFeatureRDD, 4096)
    } else {
      keyedFeatureRDD.values
    }

    /*
     * Here we're going to work with the features one partition at a time.
     * We're going to use the tile key from windowLayout to read pixels from appropriate raster.
     * Each record in this RDD may still represent only a partial result for that feature.
     *
     * The RDD is keyed by Id such that we can join and recombine partial results later.
     */
    val featuresWithSummaries: RDD[(FEATUREID, ValidatedSummary[SUMMARY])] =
      partitionedFeatureRDD.mapPartitions {
        featurePartition: Iterator[(SpatialKey, Feature[Geometry, FEATUREID])] =>
          // Code inside .mapPartitions works in an Iterator of records
          // Doing things this way allows us to reuse resources and perform other optimizations
          // Grouping by spatial key allows us to minimize read thrashing from record to record

          val groupedByKey : Map[SpatialKey, Array[Feature[Geometry, FEATUREID]]] =
            featurePartition.toArray.groupBy {
              case (windowKey, _) => windowKey
            }.mapValues(_.map{ case (_, feature) => feature })

          groupedByKey.toIterator.flatMap {
            case (windowKey, features) =>
              val maybeRasterSource: Either[JobError, SOURCES] =
                getSources(windowKey, windowLayout, kwargs)
                  .left.map(ex =>
                    RasterReadError(ex.getMessage)
                )

              val maybeRaster: Either[JobError, Raster[TILE]] =
                maybeRasterSource.flatMap { rs: SOURCES =>
                  readWindow(rs, windowKey, windowLayout)
                    .left.map(ex =>
                    RasterReadError(s"Reading raster for $windowKey: ${ex.getMessage}")
                  )
                }

              def getSummaryForGeom(geom: Geometry, mykwargs: Map[String, Any]): ValidatedSummary[SUMMARY] = {
                  val summary: Either[JobError, PolygonalSummaryResult[SUMMARY]] =
                    maybeRaster.flatMap { raster =>
                      Either.catchNonFatal {
                        runPolygonalSummary(
                          raster,
                          geom,
                          ErrorSummaryRDD.rasterizeOptions,
                          mykwargs)
                      }.left.map{
                        case NoYieldException(msg) => NoYieldError(msg)
                        case ise: java.lang.IllegalStateException =>
                          GeometryError(s"IllegalStateException")
                        case te: org.locationtech.jts.geom.TopologyException =>
                          GeometryError(s"TopologyException")
                        case be: java.lang.ArrayIndexOutOfBoundsException =>
                          GeometryError(s"ArrayIndexOutOfBoundsException")
                        case e: Throwable =>
                          GeometryError(e.getMessage)
                      }
                    }
                  // Converting to Validated so errors across partial results can be accumulated
                  // @see https://typelevel.org/cats/datatypes/validated.html#validated-vs-either
                  summary.toValidated
              }

              if (kwargs.get("includeFeatureId").isDefined) {
                // Include the featureId in the kwargs passed to the polygonalSummary
                // code. This is to handle the case where the featureId may include
                // one or more columns that are used by the analysis. In that case,
                // we can't do the optimization where we share fullWindow results
                // across features.
                val partialSummaries: Array[(FEATUREID, ValidatedSummary[SUMMARY])] =
                  features.map { feature: Feature[Geometry, FEATUREID] =>
                    val id: FEATUREID = feature.data
                    (id, getSummaryForGeom(feature.geom, kwargs + ("featureId" -> id)))
                  }
                partialSummaries
              } else {
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

                  // for partial windows, we need to calculate summary for each geometry,
                  // since they all may have unique intersections with the window
                  val partialWindowResults = partialWindowFeatures.map {
                    case feature =>
                      (feature.data, getSummaryForGeom(feature.geom, kwargs))
                  }

                  // if there are any full window intersections, we only need to calculate
                  // the summary for the window, and then tie it to each feature ID
                  val fullWindowIds = fullWindowFeatures.map { case feature => feature.data}.toList
                  val fullWindowResults =
                    if (fullWindowFeatures.nonEmpty) {
                      fullWindowIds.map { id => (id, getSummaryForGeom(windowGeom, kwargs)) }
                    } else {
                      List.empty
                    }

                  // combine results
                  partialWindowResults ++ fullWindowResults
              }
          }
      }

    /* Group records by Id and combine their summaries. The features may have intersected
     * multiple grid blocks. The combine operation for a SUMMARY is defined in
     * summaryStats.summarySemigroup, based on its merge method.
     */
    val featuresGroupedWithSummaries: RDD[ValidatedLocation[SUMMARY]] =
      featuresWithSummaries
        .reduceByKey(Semigroup.combine)
        .map { case (fid, summary) =>
          summary match {
            // If there was no intersection for any partial results, we consider this an invalid geometry
            case Valid(GTSummary(result)) if result.isEmpty =>
              Valid(Location(fid, result))
            case Invalid(error) =>
              Invalid(Location(fid, error))
            case Valid(GTSummary(result)) =>
              Valid(Location(fid, result))
          }
        }

    featuresGroupedWithSummaries
  }

  // Get the grid sources (subclass of GridSources) associated with a specified
  // window key and windowLayout.
  def getSources(key: SpatialKey, windowLayout: LayoutDefinition, kwargs: Map[String, Any]): Either[Throwable, SOURCES]

  def readWindow(rs: SOURCES, windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[TILE]]

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          kwargs: Map[String, Any]): PolygonalSummaryResult[SUMMARY]

}

object ErrorSummaryRDD {
    val rasterizeOptions: Rasterizer.Options =
      Rasterizer.Options(includePartial = false, sampleType = PixelIsPoint)
}
