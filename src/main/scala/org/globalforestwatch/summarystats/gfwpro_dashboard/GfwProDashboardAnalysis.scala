package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.NonEmptyList
import geotrellis.vector.{Feature, Geometry, MultiPolygon}
import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry, MultiPolygon => GeoSparkMultiPolygon}
import geotrellis.store.index.zcurve.Z2
import org.apache.spark.HashPartitioner
import org.globalforestwatch.features.{CombinedFeatureId, FeatureIdFactory, FeatureRDDFactory, FireAlertRDD, GfwProFeatureId, SpatialFeatureDF}
import org.globalforestwatch.summarystats.SummaryAnalysis
import org.globalforestwatch.util.IntersectGeometry.intersectGeometries
import org.globalforestwatch.util.{RDDAdapter, SpatialJoinRDD}
import org.globalforestwatch.util.ImplicitGeometryConverter._
import org.globalforestwatch.util.RDDAdapter.toSpatialRDD

import java.util
//import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
//import org.apache.sedona.core.spatialOperator.JoinQuery
//import org.apache.sedona.core.spatialRDD.PointRDD
//import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.util.Util.getAnyMapValue
import org.datasyslab.geosparksql.utils.Adapter

import scala.collection.JavaConverters._

object GfwProDashboardAnalysis extends SummaryAnalysis {

  val name = "gfwpro_dashboard"

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    val enrichedRDD = intersectWithContextualLayer(featureRDD, kwargs, spark)
    enrichedRDD.cache()

    val summaryRDD: RDD[(FeatureId, GfwProDashboardSummary)] =
      GfwProDashboardRDD(enrichedRDD, GfwProDashboardGrid.blockTileGrid, kwargs)

    val fireCountRDD: RDD[(FeatureId, GfwProDashboardDataDateCount)] =
      GfwProDashboardAnalysis.fireStats(enrichedRDD, spark, kwargs)

    val dataRDD: RDD[(FeatureId, GfwProDashboardData)] =
      formatGfwProDashboardData(summaryRDD)
        .reduceByKey(_ merge _)
        .leftOuterJoin(fireCountRDD)
        .mapValues {
          case (data, fire) =>
            data.update(
              viirsAlertsDaily =
                fire.getOrElse(GfwProDashboardDataDateCount.empty)
            )
        }

    val summaryDF =
      GfwProDashboardDFFactory(featureType, dataRDD, spark, kwargs).getDataFrame

    val runOutputUrl: String = getOutputUrl(kwargs)

    GfwProDashboardExport.export(featureType, summaryDF, runOutputUrl, kwargs)
  }

  def fireStats(
                 featureRDD: RDD[Feature[Geometry, FeatureId]],
                 spark: SparkSession,
                 kwargs: Map[String, Any]
               ): RDD[(FeatureId, GfwProDashboardDataDateCount)] = {

    // FIRE RDD

    val fireAlertSpatialRDD = FireAlertRDD(spark, kwargs)

    // Feature RDD
    val featureSpatialRDD = toSpatialRDD(featureRDD)

    val joinedRDD =
      SpatialJoinRDD.spatialjoin(featureSpatialRDD, fireAlertSpatialRDD)

    joinedRDD.rdd
      .map {
        case (poly, points) =>
          toGfwProDashboardFireData(poly, points)
      }
      .reduceByKey(_ merge _)
  }

  private def intersectWithContextualLayer(
                                            featureRDD: RDD[Feature[Geometry, FeatureId]],
                                            kwargs: Map[String, Any],
                                            spark: SparkSession
                                          ): RDD[Feature[Geometry, FeatureId]] = {

    val contextualFeatureUrl: NonEmptyList[String] =
      getAnyMapValue[NonEmptyList[String]](kwargs, "contextualFeatureUrl")
    val contextualFeatureType: String =
      getAnyMapValue[String](kwargs, "contextualFeatureType")
    val analysis: String = "gfwpro_dashboard"

    // Reading the contextual Layer directly as SpatialDF to avoid having to go via a FeatureRDD and back.
    val spatialContextualDF = SpatialFeatureDF(contextualFeatureUrl,
      contextualFeatureType,
      kwargs,
      "geom",
      spark: SparkSession
    )

    val spatialContextualRDD = Adapter.toSpatialRdd(spatialContextualDF, "polyshape")

    val spatialFeatureRDD = toSpatialRDD(featureRDD)

    val joinedRDD = SpatialJoinRDD.flatSpatialJoin(
      spatialContextualRDD,
      spatialFeatureRDD,
      considerBoundaryIntersection = true,
      usingIndex = false
    )

    val combinedFeatureRDD = joinedRDD.rdd.flatMap {
      pair: (GeoSparkGeometry, GeoSparkGeometry) =>
        val featureGeom = pair._1
        val featureId = featureGeom.getUserData.asInstanceOf[FeatureId]
        val contextualGeom = pair._2
        val contextualId = FeatureIdFactory(contextualFeatureType).fromUserData(contextualGeom.getUserData.asInstanceOf[String], delimiter = ",")

        featureId match {
          case gfwproId: GfwProFeatureId if gfwproId.locationId >= 0 =>
            if (contextualGeom.contains(featureGeom.getCentroid)) {
              Seq(
                Feature[Geometry, CombinedFeatureId](
                  featureGeom,
                  CombinedFeatureId(gfwproId, contextualId)
                )
              )
            } else Seq()
          case gfwproId: GfwProFeatureId if gfwproId.locationId < 0 =>
            val geometries = intersectGeometries(featureGeom, contextualGeom)
            geometries.flatMap { intersection: GeoSparkMultiPolygon =>
              // implicitly convert to Geotrellis geometry
              val geom: MultiPolygon = intersection
              if (geom.isEmpty) Seq()
              else
                Seq(
                  Feature[Geometry, CombinedFeatureId](
                    geom,
                    CombinedFeatureId(gfwproId, contextualId)
                  )
                )
            }
        }
    }

    // Reshuffle for later use in SummaryRDD with Range Partitioner
    val hashPartitioner = new HashPartitioner(
      combinedFeatureRDD.getNumPartitions
    )
    combinedFeatureRDD
      .keyBy({ feature: Feature[Geometry, FeatureId] =>
        Z2(
          (feature.geom.getCentroid.getX * 100).toInt,
          (feature.geom.getCentroid.getY * 100).toInt
        ).z
      })
      .partitionBy(hashPartitioner)
      .mapPartitions({ iter: Iterator[(Long, Feature[Geometry, FeatureId])] =>
        for {
          i <- iter
        } yield {
          i._2
        }
      }, preservesPartitioning = true)

  }

  private def toGfwProDashboardFireData(
                                         poly: GeoSparkGeometry,
                                         points: util.HashSet[GeoSparkGeometry]
                                       ): (FeatureId, GfwProDashboardDataDateCount) = {
    ( {
      poly.getUserData.asInstanceOf[FeatureId]
    }, {

      points.asScala.toList.foldLeft(GfwProDashboardDataDateCount.empty) {
        (z, point) => {
          // extract year from acq_date column
          val alertDate =
            point.getUserData.asInstanceOf[String].split("\t")(2)
          z.merge(
            GfwProDashboardDataDateCount
              .fill(Some(alertDate), 1, viirs = true)
          )

        }
      }

    })
  }

  private def formatGfwProDashboardData(
                                         summaryRDD: RDD[(FeatureId, GfwProDashboardSummary)]
                                       ): RDD[(FeatureId, GfwProDashboardData)] = {

    summaryRDD
      .flatMap {
        case (featureId, summary) =>
          // We need to convert the Map to a List in order to correctly flatmap the data
          summary.stats.toList.map {
            case (dataGroup, data) =>
              toGfwProDashboardData(featureId, dataGroup, data)

          }
      }
  }

  private def toGfwProDashboardData(
                                     featureId: FeatureId,
                                     dataGroup: GfwProDashboardRawDataGroup,
                                     data: GfwProDashboardRawData
                                   ): (FeatureId, GfwProDashboardData) = {

    (
      featureId,
      GfwProDashboardData(
        dataGroup.alertCoverage,
        gladAlertsDaily = GfwProDashboardDataDateCount
          .fill(dataGroup.alertDate, data.alertCount),
        gladAlertsWeekly = GfwProDashboardDataDateCount
          .fill(dataGroup.alertDate, data.alertCount, weekly = true),
        gladAlertsMonthly = GfwProDashboardDataDateCount
          .fill(dataGroup.alertDate, data.alertCount, monthly = true),
        viirsAlertsDaily = GfwProDashboardDataDateCount.empty
      )
    )

  }
}