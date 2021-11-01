package org.globalforestwatch.summarystats

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.summary.polygonal.{NoIntersection, PolygonalSummaryResult, Summary=>GTSummary}
import geotrellis.store.index.zcurve.Z2
import geotrellis.vector._
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.grids.GridSources
import scala.reflect.ClassTag
import cats.kernel.Semigroup
import cats.data.Validated.{Valid, Invalid, valid, invalid}
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticSummary


trait ErrorSummaryRDD extends LazyLogging with java.io.Serializable {

  type SOURCES <: GridSources
  type SUMMARY <: Summary[SUMMARY]
  type TILE <: CellGrid[Int]

  /** Produce RDD of tree cover loss from RDD of areas of interest*
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
  )(implicit kt: ClassTag[SUMMARY], vt: ClassTag[FEATUREID]): RDD[(FEATUREID, ValidatedRow[SUMMARY])] = {

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

    val partitionedFeatureRDD = if (partition) {
      val inputPartitionMultiplier = 64
      val rangePartitioner =
        new RangePartitioner(featureRDD.getNumPartitions * inputPartitionMultiplier, keyedFeatureRDD)
      keyedFeatureRDD.partitionBy(rangePartitioner)
    } else {
      keyedFeatureRDD
    }

    // countRecordsPerPartition(partitionedFeatureRDD, SummarySparkSession("tmp"))

    /*
     * Here we're going to work with the features one partition at a time.
     * We're going to use the tile key from windowLayout to read pixels from appropriate raster.
     * Each record in this RDD may still represent only a partial result for that feature.
     *
     * The RDD is keyed by Id such that we can join and recombine partial results later.
     */
    val featuresWithSummaries: RDD[(FEATUREID, ValidatedSummary[SUMMARY])] =
      partitionedFeatureRDD.mapPartitions {
        featurePartition: Iterator[
          (Long, (SpatialKey, Feature[Geometry, FEATUREID]))
        ] =>
          // Code inside .mapPartitions works in an Iterator of records
          // Doing things this way allows us to reuse resources and perform other optimizations
          // Grouping by spatial key allows us to minimize read thrashing from record to record

          val windowFeature = featurePartition.map {
            case (_, (windowKey, feature)) =>
              (windowKey, feature)
          }

          val groupedByKey
            : Map[SpatialKey,
                  Array[(SpatialKey, Feature[Geometry, FEATUREID])]] =
            windowFeature.toArray.groupBy {
              case (windowKey, _) => windowKey
            }

          groupedByKey.toIterator.flatMap {
            case (windowKey, keysAndFeatures) =>
              val maybeRasterSource: Either[JobError, SOURCES] =
                getSources(windowKey, windowLayout, kwargs)
                  .left.map(ex => RasterReadError(ex.getMessage))

              val features = keysAndFeatures map { case (_, feature) => feature }

              val maybeRaster: Either[JobError, Raster[TILE]] =
                maybeRasterSource.flatMap { rs: SOURCES =>
                  readWindow(rs, windowKey, windowLayout)
                    .left.map(ex => RasterReadError(s"Reading raster for $windowKey"))
                }


              val partialSummaries: Array[(FEATUREID, ValidatedSummary[SUMMARY])] =
                features.map { feature: Feature[Geometry, FEATUREID] =>
                  val id: FEATUREID = feature.data
                  val summary: Either[JobError, PolygonalSummaryResult[SUMMARY]] =
                    maybeRaster.flatMap { raster =>
                      Either.catchNonFatal {
                        runPolygonalSummary(
                          raster,
                          feature.geom,
                          ErrorSummaryRDD.rasterizeOptions,
                          kwargs)
                      }.left.map{
                        // TODO: these should be moved into left side of PolygonalSummaryResult in GT
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
                  (id, summary.toValidated)
                }

              partialSummaries
          }
      }

    /* Group records by Id and combine their summaries
     * The features may have intersected multiple grid blocks
     */
    val featuresGroupedWithSummaries: RDD[(FEATUREID, ValidatedRow[SUMMARY])] =
      featuresWithSummaries
        .reduceByKey(Semigroup.combine)
        .mapValues{
          // If there was no intersection for any partial results, we consider this an invalid geometry
          case Valid(NoIntersection) =>
            Invalid(NoIntersectionError)
          case Valid(GTSummary(result)) if result.isEmpty =>
            Invalid(NoIntersectionError)
          case Valid(GTSummary(result)) =>
            Valid(result)
          case r@Invalid(_) =>
            r
        }

    featuresGroupedWithSummaries
  }

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
