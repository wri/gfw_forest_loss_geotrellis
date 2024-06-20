package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.{NonEmptyList, Validated}
import geotrellis.vector.{Feature, Geometry}
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats._
import org.globalforestwatch.util.{RDDAdapter, SpatialJoinRDD}
import org.globalforestwatch.util.RDDAdapter
import org.globalforestwatch.ValidatedWorkflow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.globalforestwatch.features.FeatureId
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom

import scala.collection.JavaConverters._
import java.time.LocalDate
import org.globalforestwatch.util.IntersectGeometry

object GfwProDashboardAnalysis extends SummaryAnalysis {

  val name = "gfwpro_dashboard"

  def apply(
    featureRDD: RDD[ValidatedLocation[Geometry]],
    featureType: String,
    contextualFeatureType: String,
    contextualFeatureUrl: NonEmptyList[String],
    fireAlertRDD: SpatialRDD[Geometry],
    spark: SparkSession,
    kwargs: Map[String, Any]
  ): ValidatedWorkflow[Location[JobError],(FeatureId, GfwProDashboardData)] = {
    featureRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val summaryRDD = ValidatedWorkflow(featureRDD).flatMap { rdd =>
      val spatialContextualDF = SpatialFeatureDF(contextualFeatureUrl, contextualFeatureType, FeatureFilter.empty, "geom", spark)
      val spatialContextualRDD = Adapter.toSpatialRdd(spatialContextualDF, "polyshape")
      val spatialFeatureRDD = RDDAdapter.toSpatialRDDfromLocationRdd(rdd, spark)

      /* Enrich the feature RDD by intersecting it with contextual features
       * The resulting FeatureId carries combined identity of source feature and contextual geometry
       */
      val enrichedRDD =
        SpatialJoinRDD
          .flatSpatialJoin(spatialContextualRDD, spatialFeatureRDD, considerBoundaryIntersection = true, usingIndex = true)
          .rdd
          .flatMap { case (feature, context) =>
            refineContextualIntersection(feature, context, contextualFeatureType)
          }

      ValidatedWorkflow(enrichedRDD)
        .mapValidToValidated { rdd =>
          rdd.map { case row@Location(fid, geom) =>
            if (geom.isEmpty()) {
              Validated.invalid[Location[JobError], Location[Geometry]](Location(fid, GeometryError(s"Empty Geometry")))
            } else if (!geom.isValid) {
              Validated.invalid[Location[JobError], Location[Geometry]](Location(fid, GeometryError(s"Invalid Geometry")))
            } else
              Validated.valid[Location[JobError], Location[Geometry]](row)
          }
        }
        .flatMap { enrichedRDD =>
          val fireStatsRDD = fireStats(enrichedRDD, fireAlertRDD, spark)
          val tmp = enrichedRDD.map { case Location(id, geom) => Feature(geom, id) }
          val validatedSummaryStatsRdd = GfwProDashboardRDD(tmp, GfwProDashboardGrid.blockTileGrid, kwargs)
          ValidatedWorkflow(validatedSummaryStatsRdd).mapValid { summaryStatsRDD =>
            // fold in fireStatsRDD after polygonal summary and accumulate the errors
            summaryStatsRDD
              .mapValues(_.toGfwProDashboardData())
              .leftOuterJoin(fireStatsRDD)
              .mapValues { case (data, fire) =>
                data.copy(viirs_alerts_daily = fire.getOrElse(GfwProDashboardDataDateCount.empty))
              }
          }
        }
    }
    summaryRDD

  }

  /** These geometries touch, apply application specific logic of how to treat that.
    *   - For intersection of location geometries, only keep those where centroid of
    *     location is in the contextual geom (this ensures that any location is only
    *     assigned to a single contextual area even if it intersects more).
    *   - For dissolved geometry of list, report all contextual areas it intersects
    */
  private def refineContextualIntersection(
    featureGeom: Geometry,
    contextualGeom: Geometry,
    contextualFeatureType: String
  ): List[ValidatedLocation[Geometry]] = {
    val featureId = featureGeom.getUserData.asInstanceOf[FeatureId]
    val contextualId = FeatureId.fromUserData(contextualFeatureType, contextualGeom.getUserData.asInstanceOf[String], delimiter = ",")

    featureId match {
      // ValidateFeatureRDD already computed the centroid of the location (when
      // locationId != -1) and stashed it in the FeatureId.
      case CombinedFeatureId(gfwproId: GfwProFeatureId, featureCentroid: PointFeatureId) if gfwproId.locationId >= 0 =>
        if (contextualGeom.contains(featureCentroid.pt)) {
          val fid = CombinedFeatureId(gfwproId, contextualId)
          // val gtGeom: Geometry = toGeotrellisGeometry(featureGeom)
          val fixedGeom = makeValidGeom(featureGeom)
          List(Validated.Valid(Location(fid, fixedGeom)))
        } else Nil

      case CombinedFeatureId(gfwproId: GfwProFeatureId, _) if gfwproId.locationId < 0 =>
        IntersectGeometry
          .validatedIntersection(featureGeom, contextualGeom)
          .leftMap { err => Location(featureId, err) }
          .map { geometries =>
            geometries.map { geom =>
              // val gtGeom: Geometry = toGeotrellisGeometry(geom)
              val fixedGeom = makeValidGeom(geom)
              Location(CombinedFeatureId(gfwproId, contextualId), fixedGeom)
            }
          }
          .traverse(identity) // turn validated list of geometries into list of validated geometries
    }
  }

  private def fireStats(
    featureRDD: RDD[Location[Geometry]],
    fireAlertRDD: SpatialRDD[Geometry],
    spark: SparkSession
  ): RDD[Location[GfwProDashboardDataDateCount]] = {
    val featureSpatialRDD = RDDAdapter.toSpatialRDDfromLocationRdd(featureRDD, spark)
    // If there are no locations that intersect the TCL extent (featureSpatialRDD is
    // empty, has no envelope), then spatial join below will fail, so return without
    // further analysis.
    if (featureSpatialRDD.boundaryEnvelope == null) {
      return spark.sparkContext.parallelize(Seq.empty[Location[GfwProDashboardDataDateCount]])
    }
    val joinedRDD = SpatialJoinRDD.spatialjoin(featureSpatialRDD, fireAlertRDD)

    joinedRDD.rdd
      .map { case (poly, points) =>
        val fid = poly.getUserData.asInstanceOf[FeatureId]
        val data = points.asScala.foldLeft(GfwProDashboardDataDateCount.empty) { (z, point) =>
          // extract year from acq_date column is YYYY-MM-DD
          val acqDate = point.getUserData.asInstanceOf[String].split("\t")(2)
          val alertDate = LocalDate.parse(acqDate)
          z.merge(GfwProDashboardDataDateCount.fillDaily(Some(alertDate), true, 1))
        }
        (fid, data)
      }
      .reduceByKey(_ merge _)
  }
}
