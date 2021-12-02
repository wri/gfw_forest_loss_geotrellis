package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.{NonEmptyList, Validated}
import geotrellis.vector.{Feature, Geometry, MultiPolygon}
import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry, MultiPolygon => GeoSparkMultiPolygon}
import geotrellis.store.index.zcurve.Z2
import org.apache.spark.HashPartitioner
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats._
import org.globalforestwatch.util.GeoSparkGeometryConstructor.createPoint
import org.globalforestwatch.util.{RDDAdapter, SpatialJoinRDD}
import org.globalforestwatch.util.ImplicitGeometryConverter._
import org.globalforestwatch.util.RDDAdapter
import org.globalforestwatch.ValidatedWorkflow

import java.util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util.getAnyMapValue
import org.datasyslab.geosparksql.utils.Adapter
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import scala.collection.JavaConverters._
import java.time.LocalDate
import org.globalforestwatch.util.IntersectGeometry
import scala.reflect.ClassTag

object GfwProDashboardAnalysis extends SummaryAnalysis {

  val name = "gfwpro_dashboard"

  def apply(
    featureRDD: RDD[ValidatedLocation[Geometry]],
    featureType: String,
    contextualFeatureType: String,
    contextualFeatureUrl: NonEmptyList[String],
    fireAlertRDD: SpatialRDD[GeoSparkGeometry],
    spark: SparkSession,
    kwargs: Map[String, Any]
  ): Unit = {
    featureRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val summaryRDD = ValidatedWorkflow(featureRDD).flatMap { rdd =>
      val spatialContextualDF = SpatialFeatureDF(contextualFeatureUrl, contextualFeatureType, FeatureFilter.empty, "geom", spark)
      val spatialContextualRDD = Adapter.toSpatialRdd(spatialContextualDF, "polyshape")
      val spatialFeatureRDD = RDDAdapter.toSpatialRDDfromLocationRdd(rdd)

      /* Enrich the feature RDD by intersecting it with contextual features
       * The resulting FeatuerId carries combined identity of source fature and contextual geometry
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
            if (geom.isEmpty())
              Validated.invalid[Location[JobError], Location[Geometry]](Location(fid, GeometryError(s"Empty Geometry")))
            else
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

    val summaryDF = GfwProDashboardDF.getFeatureDataFrameFromVerifiedRdd(summaryRDD.unify, spark)
    val runOutputUrl: String = getOutputUrl(kwargs)
    GfwProDashboardExport.export(featureType, summaryDF, runOutputUrl, kwargs)

  }

  /** These geometries touch, apply application specific logic of how to treat that.
    *   - For intersection of location geometries only keep those where centroid of location is in the contextual geom (this ensures that
    *     any location is only assigned to a single contextual area even if it intersects more)
    *   - For dissolved geometry of list report all contextual areas it intersects
    */
  private def refineContextualIntersection(
    featureGeom: GeoSparkGeometry,
    contextualGeom: GeoSparkGeometry,
    contextualFeatureType: String
  ): List[ValidatedLocation[Geometry]] = {
    val featureId = featureGeom.getUserData.asInstanceOf[FeatureId]
    val contextualId = FeatureId.fromUserData(contextualFeatureType, contextualGeom.getUserData.asInstanceOf[String], delimiter = ",")

    featureId match {
      case gfwproId: GfwProFeatureId if gfwproId.locationId >= 0 =>
        val featureCentroid = createPoint(gfwproId.x, gfwproId.y)
        if (contextualGeom.contains(featureCentroid)) {
          val fid = CombinedFeatureId(gfwproId, contextualId)
          val gtGeom: Geometry = toGeotrellisGeometry(featureGeom)
          List(Validated.Valid(Location(fid, gtGeom)))
        } else Nil

      case gfwproId: GfwProFeatureId if gfwproId.locationId < 0 =>
        IntersectGeometry
          .validatedIntersection(featureGeom, contextualGeom)
          .leftMap { err => Location(featureId, err) }
          .map { geometries =>
            geometries.map { geom =>
              val gtGeom: Geometry = toGeotrellisGeometry(geom)
              Location(CombinedFeatureId(gfwproId, contextualId), gtGeom)
            }
          }
          .traverse(identity) // turn validated list of geometries into list of validated geometries
    }
  }

  private def partitionByZIndex[A: ClassTag](rdd: RDD[A])(getGeom: A => Geometry): RDD[A] = {
    val hashPartitioner = new HashPartitioner(rdd.getNumPartitions)

    rdd
      .keyBy({ row =>
        val geom = getGeom(row)
        Z2(
          (geom.getCentroid.getX * 100).toInt,
          (geom.getCentroid.getY * 100).toInt
        ).z
      })
      .partitionBy(hashPartitioner)
      .mapPartitions(
        { iter: Iterator[(Long, A)] =>
          for (i <- iter) yield i._2
        },
        preservesPartitioning = true
      )
  }

  private def fireStats(
    featureRDD: RDD[Location[Geometry]],
    fireAlertRDD: SpatialRDD[GeoSparkGeometry],
    spark: SparkSession
  ): RDD[Location[GfwProDashboardDataDateCount]] = {
    val featureSpatialRDD = RDDAdapter.toSpatialRDDfromLocationRdd(featureRDD)
    val joinedRDD = SpatialJoinRDD.spatialjoin(featureSpatialRDD, fireAlertRDD)

    joinedRDD.rdd
      .map { case (poly, points) =>
        val fid = poly.getUserData.asInstanceOf[FeatureId]
        val data = points.asScala.foldLeft(GfwProDashboardDataDateCount.empty) { (z, point) =>
          // extract year from acq_date column is YYYY-MM-DD
          val acqDate = point.getUserData.asInstanceOf[String].split("\t")(2)
          val alertDate = LocalDate.parse(acqDate)
          z.merge(GfwProDashboardDataDateCount.fillDaily(Some(alertDate), 1))
        }
        (fid, data)
      }
      .reduceByKey(_ merge _)
  }
}
