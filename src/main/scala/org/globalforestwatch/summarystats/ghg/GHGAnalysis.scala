package org.globalforestwatch.summarystats.ghg

import cats.data.Validated.{Invalid, Valid}
import cats.implicits._

import geotrellis.vector.{Feature, Geometry}
import org.locationtech.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.summarystats.{Location, NoIntersectionError, SummaryAnalysis, ValidatedLocation}
import org.apache.spark.storage.StorageLevel
import org.globalforestwatch.ValidatedWorkflow
import org.globalforestwatch.features.GfwProFeatureExtId

object GHGAnalysis extends SummaryAnalysis {

  val name = "ghg"

  /** Greenhouse gas analysis of input features in a TSV file. The TSV file contains
    * the individual list items (location IDs >= 0). Any option merged ("dissolved")
    * list geometries (location id -1) are ignored.
    *
    * This function assumes that all features have already been split by 1x1 degree
    * grid, so each location and merged list may have a single or multiple rows.
    */
  def apply(
    features: RDD[ValidatedLocation[Geometry]],
    kwargs: Map[String, Any]
  )(implicit spark: SparkSession): RDD[ValidatedLocation[GHGData]] = {
    features.persist(StorageLevel.MEMORY_AND_DISK)

    // Drop any rows with location id of -1 (dissolved area of whole list), since
    // that doesn't make sense for GHG analysis, since different locations on a list
    // can have different commodities or yields that are used in the analysis.
    val dropDissolvedRowsRDD = features.filter{
      case Valid(Location(GfwProFeatureExtId(_, locationId, _, _), geom)) => {
        locationId != -1
      }
    }
    try {
      val partialResult: RDD[ValidatedLocation[GHGData]] = {
        ValidatedWorkflow(dropDissolvedRowsRDD)
          .flatMap { locationGeometries =>
            val locationSummaries: RDD[ValidatedLocation[GHGSummary]] = {
              val tmp = locationGeometries.map { case Location(id, geom) => Feature(geom, id) }

              // This is where the main analysis happens, in ErrorSummaryRDD.apply(),
              // which eventually calls into GHGSummary via runPolygonalSummary().
              GHGRDD(tmp, GHGGrid.blockTileGrid, kwargs)
            }

            // For all rows that didn't get an error from the GHG analysis, do the
            // transformation from GHGSummary to GHGData
            ValidatedWorkflow(locationSummaries).mapValid { summaries =>
              summaries
                .mapValues {
                  case summary: GHGSummary =>
                    val data = summary.toGHGData()
                    data
                }
            }
          }
          .unify
          .persist(StorageLevel.MEMORY_AND_DISK)
      }

    // If a location has empty GHGData results, then the geometry
    // must not have intersected the centroid of any pixels, so report the location
    // as NoIntersectionError.
      partialResult.map {
        case Valid(Location(fid, data)) if data.equals(GHGData.empty) =>
          Invalid(Location(fid, NoIntersectionError))
        case data => data
      }
    } catch {
      case e: StackOverflowError =>
        e.printStackTrace()
        throw e
    }
  }
}
