package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid, invalid, valid}
import cats.syntax._
import cats.implicits._

import java.util
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import geotrellis.vector.{Feature, Geometry}
import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.globalforestwatch.features.{
  CombinedFeatureId, FeatureId, FireAlertRDD, GadmFeatureId, GfwProFeature, GfwProFeatureId, GridId, WdpaFeatureId
}
import org.globalforestwatch.grids.GridId.pointGridId
import org.globalforestwatch.summarystats.{JobError, MultiError, SummaryAnalysis, ValidatedRow, ValidatedLocation, Location}
import org.globalforestwatch.util.SpatialJoinRDD
import org.globalforestwatch.util.ImplicitGeometryConverter._
import org.globalforestwatch.util.Util.getAnyMapValue
import org.apache.spark.storage.StorageLevel
import org.globalforestwatch.features.GadmFeature
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
    featureType: String,
    features: RDD[ValidatedLocation[Geometry]],
    intermediateListSource: Option[NonEmptyList[String]],
    fireAlerts: SpatialRDD[GeoSparkGeometry],
    runOutputUrl: String,
    kwargs: Map[String, Any]
  )(implicit spark: SparkSession): Unit = {
    features.persist(StorageLevel.MEMORY_AND_DISK)

    val diffGridIds: List[GridId] =
      if (intermediateListSource.nonEmpty) collectDiffGridIds(features)
      else List.empty

    val partialResult: RDD[ValidatedLocation[ForestChangeDiagnosticData]] =
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
              .mapValues { _.toForestChangeDiagnosticData().withUpdatedCommodityRisk() }
              .leftOuterJoin(fireCount)
              .mapValues { case (data, fire) =>
                data.copy(
                  commodity_threat_fires = fire.getOrElse(ForestChangeDiagnosticDataLossYearly.empty),
                  tree_cover_loss_soy_yearly = data.tree_cover_loss_soy_yearly.limitToMaxYear(2019)
                )
              }
          }
        }
        .unify
        .persist(StorageLevel.MEMORY_AND_DISK)

    val finalResult = intermediateListSource match {
      case Some(source) =>
        val intermidateResults = readIntermidateRDD(source, gridFilter = diffGridIds)
        val mergedResults = partialResult.union(intermidateResults)
        ForestChangeDiagnosticExport.export(
          "intermediate",
          ForestChangeDiagnosticDF.getGridFeatureDataFrame(mergedResults, spark),
          runOutputUrl,
          kwargs
        )
        combineGridResults(mergedResults)

      case None =>
        combineGridResults(partialResult)
    }

    ForestChangeDiagnosticExport.export(
      featureType,
      ForestChangeDiagnosticDF.getFeatureDataFrame(finalResult, spark),
      runOutputUrl,
      kwargs
    )

   features.unpersist()
   partialResult.unpersist()
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
    rdd
      .collect {
        case Valid(Location(GfwProFeatureId(_, locationId, _, _), geom)) if locationId == -2 =>
          GridId(pointGridId(geom.getCentroid, 1))
      }
      .collect
      .toList
  }

  /** Read intermidate results (per grid cell). Exclude cells present in gridFilter, those values are now outdated.
    */
  def readIntermidateRDD(
    sources: NonEmptyList[String],
    gridFilter: List[GridId]
  )(implicit spark: SparkSession): RDD[ValidatedLocation[ForestChangeDiagnosticData]] = {
    ForestChangeDiagnosticDF
      .readIntermidateRDD(sources, spark)
      .filter {
        case Valid(Location(CombinedFeatureId(fid1, fid2), _)) =>
          !gridFilter.contains(fid2)
        case Invalid(Location(CombinedFeatureId(fid1, fid2), _)) =>
          !gridFilter.contains(fid2)
        case _ =>
          false
      }
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
      }
      .reduceByKey(_ combine _)
      .map {
        case (fid, Valid(data)) =>
          Valid(Location(fid, data.withUpdatedCommodityRisk()))
        case (fid, Invalid(err)) =>
          Invalid(Location(fid, err))
      }
  }

  def fireStats(
    featureRDD: RDD[Location[Geometry]],
    fireAlertRDD: SpatialRDD[GeoSparkGeometry],
    spark: SparkSession
  ): RDD[Location[ForestChangeDiagnosticDataLossYearly]] = {
    // Convert FeatureRDD to SpatialRDD
    val polyRDD = featureRDD.map { case Location(fid, geom) =>
      val gsGeom = toGeoSparkGeometry[Geometry, GeoSparkGeometry](geom)
      gsGeom.setUserData(fid)
      gsGeom
    }
    val spatialFeatureRDD = new SpatialRDD[GeoSparkGeometry]
    spatialFeatureRDD.rawSpatialRDD = polyRDD.toJavaRDD()
    spatialFeatureRDD.fieldNames = seqAsJavaList(List("FeatureId"))
    spatialFeatureRDD.analyze()

    val joinedRDD =
      SpatialJoinRDD.spatialjoin(
        spatialFeatureRDD,
        fireAlertRDD,
        usingIndex = true
      )

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
        aggregateFireData(fires.merge(ForestChangeDiagnosticDataLossYearly.prefilled)).limitToMaxYear(2019)
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
