package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.NonEmptyList
import geotrellis.vector.{Feature, Geometry}
import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
import org.apache.log4j.Logger
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.features.{FeatureIdFactory, FireAlertRDD, SpatialFeatureDF}
import org.globalforestwatch.util.SpatialJoinRDD

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
//import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
//import org.apache.sedona.core.spatialOperator.JoinQuery
//import org.apache.sedona.core.spatialRDD.PointRDD
//import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{FeatureFactory, FeatureId}
import org.globalforestwatch.util.Util.getAnyMapValue

import scala.collection.JavaConverters._

object GfwProDashboardAnalysis {

  val logger = Logger.getLogger("GfwProDashboardAnalysis")

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    val summaryRDD: RDD[(FeatureId, GfwProDashboardSummary)] =
      GfwProDashboardRDD(featureRDD, GfwProDashboardGrid.blockTileGrid, kwargs)

    val fireCount: RDD[(FeatureId, GfwProDashboardDataDateCount)] =
      GfwProDashboardAnalysis.fireStats(featureType, spark, kwargs)

    val dataRDD: RDD[(FeatureId, GfwProDashboardData)] =
      formatGfwProDashboardData(summaryRDD)
        .reduceByKey(_ merge _)
        .leftOuterJoin(fireCount)
        .mapValues {
          case (data, fire) =>
            data.update(
              viirsAlertsDaily =
                fire.getOrElse(GfwProDashboardDataDateCount.empty)
            )
        }

    val summaryDF =
      GfwProDashboardDFFactory(featureType, dataRDD, spark, kwargs).getDataFrame

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/gfwpro_dashboard_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    GfwProDashboardExport.export(featureType, summaryDF, runOutputUrl, kwargs)
  }

  def fireStats(
                 featureType: String,
                 spark: SparkSession,
                 kwargs: Map[String, Any]
               ): RDD[(FeatureId, GfwProDashboardDataDateCount)] = {

    // FIRE RDD

    val fireAlertSpatialRDD = FireAlertRDD(spark, kwargs)

    // Feature RDD
    val featureUris: NonEmptyList[String] =
      getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris")
    val featurePolygonDF =
      SpatialFeatureDF(
        featureUris,
        featureType,
        kwargs,
        "geom",
        spark,
      )
    val featureSpatialRDD = Adapter.toSpatialRdd(featurePolygonDF, "polyshape")

    featureSpatialRDD.analyze()

    val joinedRDD = SpatialJoinRDD.spatialjoin(featureSpatialRDD, fireAlertSpatialRDD)

    joinedRDD.rdd
      .map {
        case (poly, points) =>
          toGfwProDashboardFireData(featureType, poly, points)
      }
      .reduceByKey(_ merge _)
  }

  private def toGfwProDashboardFireData(
                                         featureType: String,
                                         poly: GeoSparkGeometry,
                                         points: util.HashSet[GeoSparkGeometry]
                                       ): (FeatureId, GfwProDashboardDataDateCount) = {
    ( {
      val id =
        poly.getUserData.asInstanceOf[String].filterNot("[]".toSet).toInt
      FeatureIdFactory(featureType).featureId(id)
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
