package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.Validated.{Invalid, Valid}
import cats.implicits._

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import geotrellis.vector.{Feature, Geometry}
import org.locationtech.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.summarystats.{Location, NoIntersectionError, SummaryAnalysis, ValidatedLocation}
import org.globalforestwatch.util.SpatialJoinRDD
import org.apache.spark.storage.StorageLevel
import org.globalforestwatch.ValidatedWorkflow

object ForestChangeDiagnosticAnalysis extends SummaryAnalysis {

  val name = "forest_change_diagnostic"

  /** GFW Pro analysis of input features in a TSV file. The TSV file contains
    * the individual list items and the merged ("dissolved") list geometry.
    *   - Individual list items have location IDs >= 0
    *   - Merged list geometry has location ID -1
    *
    * The merged list may or may be not present.
    *
    * This function assumes that all features have already been split by 1x1 degree
    * grid, so each location and merged list may have a single or multiple rows.
    */
  def apply(
    features: RDD[ValidatedLocation[Geometry]],
    fireAlerts: SpatialRDD[Geometry],
    kwargs: Map[String, Any]
  )(implicit spark: SparkSession): RDD[ValidatedLocation[ForestChangeDiagnosticData]] = {
    features.persist(StorageLevel.MEMORY_AND_DISK)

    try {
      val partialResult: RDD[ValidatedLocation[ForestChangeDiagnosticData]] = {
        ValidatedWorkflow(features)
          .flatMap { locationGeometries =>
            val fireCount: RDD[Location[ForestChangeDiagnosticDataLossYearly]] =
              fireStats(locationGeometries, fireAlerts, spark)

            val locationSummaries: RDD[ValidatedLocation[ForestChangeDiagnosticSummary]] = {
              val tmp = locationGeometries.map { case Location(id, geom) => Feature(geom, id) }

              // This is where the main analysis happens, in ErrorSummaryRDD.apply(),
              // which eventually calls into ForestChangeDiagnosticSummary via
              // runPolygonalSummary().
              ForestChangeDiagnosticRDD(tmp, ForestChangeDiagnosticGrid.blockTileGrid, kwargs)
            }

            // For all rows that didn't get an error from the FCD analysis, do the
            // transformation from ForestChangeDiagnosticSummary to
            // ForestChangeDiagnosticData and add commodity risk and
            // commodity_threat_fires (both used by the Palm Risk tool), and
            // tree_cover_loss_soy_yearly.
            ValidatedWorkflow(locationSummaries).mapValid { summaries =>
              summaries
                .mapValues {
                  case summary: ForestChangeDiagnosticSummary =>
                    val data = summary.toForestChangeDiagnosticData()
                    if (data.equals(ForestChangeDiagnosticData.empty)) {
                      ForestChangeDiagnosticData.empty
                    } else {
                      data.withUpdatedCommodityRisk()
                    }
                }
                .leftOuterJoin(fireCount)
                .mapValues { case (data, fire) =>
                  if (data.equals(ForestChangeDiagnosticData.empty)) {
                    ForestChangeDiagnosticData.empty
                  } else {
                    data.copy(
                      commodity_threat_fires = fire.getOrElse(ForestChangeDiagnosticDataLossYearly.empty),
                      // Soy is planted late in year (Sept/Oct) and harvested in
                      // March. So, the most recent data relates to soy planted late
                      // in previous year. So, we should only intersect with tree
                      // cover loss from previous year.
                      tree_cover_loss_soy_yearly = data.tree_cover_loss_soy_yearly.limitToMaxYear(2023)
                    )
                  }
                }
            }
          }
          .unify
          .persist(StorageLevel.MEMORY_AND_DISK)
      }

    // If a location has empty ForestChangeDiagnosticData results, then the geometry
    // must not have intersected the centroid of any pixels, so report the location
    // as NoIntersectionError.
      partialResult.map {
        case Valid(Location(fid, data)) if data.equals(ForestChangeDiagnosticData.empty) =>
          Invalid(Location(fid, NoIntersectionError))
        case data => data
      }
    } catch {
      case e: StackOverflowError =>
        e.printStackTrace()
        throw e
    }
  }

  def fireStats(
    featureRDD: RDD[Location[Geometry]],
    fireAlertRDD: SpatialRDD[Geometry],
    spark: SparkSession
  ): RDD[Location[ForestChangeDiagnosticDataLossYearly]] = {
    // Convert FeatureRDD to SpatialRDD
    val polyRDD = featureRDD.map { case Location(fid, geom) =>
      geom.setUserData(fid)
      geom
    }
    val spatialFeatureRDD = new SpatialRDD[Geometry]
    spatialFeatureRDD.rawSpatialRDD = polyRDD.toJavaRDD()
    spatialFeatureRDD.fieldNames = seqAsJavaList(List("FeatureId"))
    spatialFeatureRDD.analyze()

    // If there are no locations that intersect the TCL extent (spatialFeatureRDD is
    // empty, has no envelope), then spatial join below will fail, so return without
    // further analysis.
    if (spatialFeatureRDD.boundaryEnvelope == null) {
      return spark.sparkContext.parallelize(Seq.empty[Location[ForestChangeDiagnosticDataLossYearly]])
    }

    val joinedRDD =
      SpatialJoinRDD.spatialjoin(
        spatialFeatureRDD,
        fireAlertRDD,
        usingIndex = true
      )

    // This fire data is an input to the palm risk tool, so limit data to 2023 to sync
    // with the palm risk tool.
    joinedRDD.rdd
      .map { case (poly, points) =>
        val fid = poly.getUserData.asInstanceOf[FeatureId]
        val fireCount = points.asScala.foldLeft(SortedMap.empty[Int, Double]) { (acc, point) =>
          // extract year from acq_date column
          val year = point.getUserData
            .asInstanceOf[String]
            .split("\t")(2)
            .substring(0, 4)
            .toInt
          val count = acc.getOrElse(year, 0.0) + 1.0
          acc.updated(year, count)
        }
        (fid, ForestChangeDiagnosticDataLossYearly(fireCount))
      }
      .reduceByKey(_ merge _)
      .mapValues { fires =>
        aggregateFireData(fires.merge(ForestChangeDiagnosticDataLossYearly.prefilled)).limitToMaxYear(2023)
      }
  }

  def aggregateFireData(
    fires: ForestChangeDiagnosticDataLossYearly
  ): ForestChangeDiagnosticDataLossYearly = {
    val minFireYear = fires.value.keysIterator.min
    val maxFireYear = fires.value.keysIterator.max
    val years: List[Int] = List.range(minFireYear + 1, maxFireYear + 1)

    ForestChangeDiagnosticDataLossYearly(
      SortedMap(
        years.map(year =>
          (
            year, {
              val thisYearFireCount: Double = fires.value.getOrElse(year, 0)
              val lastYearFireCount: Double = fires.value.getOrElse(year - 1, 0)
              (thisYearFireCount + lastYearFireCount) / 2
            }
          )
        ): _*
      )
    )
  }
}
