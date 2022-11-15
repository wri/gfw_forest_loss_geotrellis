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
import org.globalforestwatch.features.{CombinedFeatureId, FeatureId, GfwProFeatureId, GridId}
import org.globalforestwatch.grids.GridId.pointGridId
import org.globalforestwatch.summarystats.{Location, NoIntersectionError, SummaryAnalysis, ValidatedLocation}
import org.globalforestwatch.util.SpatialJoinRDD
import org.apache.spark.storage.StorageLevel
import org.globalforestwatch.ValidatedWorkflow

object ForestChangeDiagnosticAnalysis extends SummaryAnalysis {

  val name = "forest_change_diagnostic"

  /** GFW Pro hand of a input features in a TSV file TSV file contains the individual list items, the merged list geometry and the
    * geometric difference from the current merged list geometry and the former one.
    *   - Individual list items have location IDs >= 0
    *   - Merged list geometry has location ID -1
    *   - Geometric difference to previous version has location ID -2
    *
    * Merged list and geometric difference may or may be not present. If geometric difference is present, we only need to process chunks
    * of the merged list which fall into the same grid cells as the geometric difference. Later in the analysis we will then read cached
    * values for the remaining chunks and use them to aggregate list level results.
    *
    * This function assumes that all features have already been split by 1x1 degree grid. This function will exclude diff geometry
    * locations from output (id=-2).
    */
  def apply(
    features: RDD[ValidatedLocation[Geometry]],
    intermediateResultsRDD: Option[RDD[ValidatedLocation[ForestChangeDiagnosticData]]],
    fireAlerts: SpatialRDD[Geometry],
    saveIntermidateResults: RDD[ValidatedLocation[ForestChangeDiagnosticData]] => Unit,
    kwargs: Map[String, Any]
  )(implicit spark: SparkSession): RDD[ValidatedLocation[ForestChangeDiagnosticData]] = {
    features.persist(StorageLevel.MEMORY_AND_DISK)

    try {
      val diffGridIds: List[GridId] =
        if (intermediateResultsRDD.nonEmpty) collectDiffGridIds(features)
        else List.empty

      // These records are not covered by diff geometry, they're still valid and can be re-used
      val cachedIntermidateResultsRDD = intermediateResultsRDD.map { rdd =>
        rdd.filter {
          case Valid(Location(CombinedFeatureId(fid1, fid2), _)) =>
            !diffGridIds.contains(fid2)
          case Invalid(Location(CombinedFeatureId(fid1, fid2), _)) =>
            !diffGridIds.contains(fid2)
          case _ =>
            false
        }
      }

      val partialResult: RDD[ValidatedLocation[ForestChangeDiagnosticData]] = {
        ValidatedWorkflow(features)
          .flatMap { locationGeometries =>
            val diffLocations = filterDiffGridCells(locationGeometries, diffGridIds)

            val fireCount: RDD[Location[ForestChangeDiagnosticDataLossYearly]] =
              fireStats(diffLocations, fireAlerts, spark)

            val locationSummaries: RDD[ValidatedLocation[ForestChangeDiagnosticSummary]] = {
              val tmp = diffLocations.map { case Location(id, geom) => Feature(geom, id) }
              ForestChangeDiagnosticRDD(tmp, ForestChangeDiagnosticGrid.blockTileGrid, kwargs)
            }

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
                      tree_cover_loss_soy_yearly = data.tree_cover_loss_soy_yearly.limitToMaxYear(2021)
                    )
                  }
                }
            }
          }
          .unify
          .persist(StorageLevel.MEMORY_AND_DISK)
      }

      cachedIntermidateResultsRDD match {
        case Some(cachedResults) =>
          val mergedResults = partialResult.union(cachedResults)
          saveIntermidateResults(mergedResults)
          combineGridResults(mergedResults)
        case None =>
          combineGridResults(partialResult)
      }
    } catch {
      case e: StackOverflowError =>
        e.printStackTrace()
        throw e
    }
  }

  /** Filter only to those rows covered by gridFilter, these are areas where location geometries have changed If gridFilter is empty list,
    * all locations except diff geom will be preserved
    */
  def filterDiffGridCells(
    rdd: RDD[Location[Geometry]],
    gridFilter: List[GridId]
  ): RDD[Location[Geometry]] = {
    def keepLocationCell(locationId: Int, geom: Geometry): Boolean =
      (locationId >= -1) && (gridFilter.isEmpty || gridFilter.contains(GridId(pointGridId(geom.getCentroid, 1))))

    rdd.collect {
      case Location(gfwFid @ GfwProFeatureId(_, lid, _, _), geom) if keepLocationCell(lid, geom) =>
        val grid = pointGridId(geom.getCentroid, 1)
        val fid = CombinedFeatureId(gfwFid, GridId(grid))
        Location(fid, geom)
    }
  }

  /** Collect lists of GridIds for which diff geometry is present (id=-2) */
  def collectDiffGridIds(rdd: RDD[ValidatedLocation[Geometry]]): List[GridId] = {
    // new logic: get ID with new old geom, get grid IDs, join on same grid ID, collect
    // IDs where grid geometry is not the same
    rdd
      .collect {
        case Valid(Location(GfwProFeatureId(_, locationId, _, _), geom)) if locationId == -1 =>
          GridId(pointGridId(geom.getCentroid, 1))
      }
      .collect
      .toList
  }

  /** Combine per grid results named by CombinedFeatureId to per location results named by FeatureId Some of the per-grid results fo may
    * be Invalid errors. Combining per-grid results will aggregate errors up to Location level.
    */
  def combineGridResults(
    rdd: RDD[ValidatedLocation[ForestChangeDiagnosticData]]
  )(implicit spark: SparkSession): RDD[ValidatedLocation[ForestChangeDiagnosticData]] = {
    rdd
      .map {
        case Valid(Location(CombinedFeatureId(fid, _), data)) =>
          (fid, Valid(data))
        case Invalid(Location(CombinedFeatureId(fid, _), err)) =>
          (fid, Invalid(err))
        case Valid(Location(fid, data)) =>
          (fid, Valid(data))
        case Invalid(Location(fid, err)) =>
          (fid, Invalid(err))
      }
      .reduceByKey(_ combine _)
      .map {
        case (fid, Valid(data)) if data.equals(ForestChangeDiagnosticData.empty) =>
          Invalid(Location(fid, NoIntersectionError))
        case (fid, Valid(data)) =>
          Valid(Location(fid, data.withUpdatedCommodityRisk()))
        case (fid, Invalid(err)) =>
          Invalid(Location(fid, err))
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

    val joinedRDD =
      SpatialJoinRDD.flatSpatialJoin(
        fireAlertRDD,
        spatialFeatureRDD,
        usingIndex = true
      )

    joinedRDD.rdd
      .groupBy({
        case p: (Geometry, Geometry) => p._2
      })
      .mapValues({
        case geoms: Iterable[(Geometry, Geometry)] => geoms.map(g => g._1)
      })
      .map {
        case (poly, points) =>
          val fid = poly.getUserData.asInstanceOf[FeatureId]
          val fireCount = points.foldLeft(SortedMap.empty[Int, Double]) { (acc, point) =>
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
        aggregateFireData(fires.merge(ForestChangeDiagnosticDataLossYearly.prefilled)).limitToMaxYear(2021)
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
