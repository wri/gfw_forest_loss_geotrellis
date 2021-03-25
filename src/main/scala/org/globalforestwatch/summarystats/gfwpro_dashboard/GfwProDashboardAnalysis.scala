package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.NonEmptyList
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.Logger
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geosparksql.utils.Adapter

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
//import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
//import org.apache.sedona.core.spatialOperator.JoinQuery
//import org.apache.sedona.core.spatialRDD.PointRDD
//import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{FeatureDF, FeatureFactory, FeatureId, SimpleFeatureId}
import org.globalforestwatch.util.Util.getAnyMapValue

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

object GfwProDashboardAnalysis {

  val logger = Logger.getLogger("GfwProDashboardAnalysis")

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {


    val summaryRDD: RDD[(FeatureId, GfwProDashboardSummary)] =
      GfwProDashboardRDD(
        featureRDD,
        GfwProDashboardGrid.blockTileGrid,
        kwargs
      )

    val summaryDF =
      GfwProDashboardDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/gfwpro_dashboard_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    GfwProDashboardExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }

  def fireStats(
                 featureType: String,
                 spark: SparkSession,
                 kwargs: Map[String, Any]
               ): RDD[(SimpleFeatureId, GfwProDashboardDataDateCount)] = {

    // FIRE RDD
    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val fireAlertUris
    : NonEmptyList[String] = getAnyMapValue[Option[NonEmptyList[String]]](
      kwargs,
      "fireAlertSource"
    ) match {
      case None =>
        throw new java.lang.IllegalAccessException(
          "fire_alert_source parameter required for fire alerts analysis"
        )
      case Some(s: NonEmptyList[String]) => s
    }
    val fireAlertObj =
      FeatureFactory("firealerts", Some(fireAlertType)).featureObj
    val fireAlertPointDF = FeatureDF(
      fireAlertUris,
      fireAlertObj,
      kwargs,
      spark,
      "longitude",
      "latitude"
    )
    val fireAlertSpatialRDD =
      Adapter.toSpatialRdd(fireAlertPointDF, "pointshape")

    fireAlertSpatialRDD.analyze()

    // Feature RDD
    val featureObj = FeatureFactory(featureType).featureObj
    val featureUris: NonEmptyList[String] =
      getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris")
    val featurePolygonDF =
      FeatureDF(featureUris, featureObj, featureType, kwargs, spark, "geom")
    val featureSpatialRDD = Adapter.toSpatialRdd(featurePolygonDF, "polyshape")

    featureSpatialRDD.analyze()

    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
    val usingIndex = false

    fireAlertSpatialRDD.spatialPartitioning(GridType.QUADTREE)

    featureSpatialRDD.spatialPartitioning(fireAlertSpatialRDD.getPartitioner)
    featureSpatialRDD.buildIndex(
      IndexType.QUADTREE,
      buildOnSpatialPartitionedRDD
    )

    val pairRDD = JoinQuery.SpatialJoinQuery(
      fireAlertSpatialRDD,
      featureSpatialRDD,
      usingIndex,
      considerBoundaryIntersection
    )

    pairRDD.rdd
      .map {
        case (poly, points) =>
          ( {
            val id =
              poly.getUserData.asInstanceOf[String].filterNot("[]".toSet).toInt
            SimpleFeatureId(id)
          }, {

            points.asScala.toList.foldLeft(GfwProDashboardDataDateCount.empty) {
              (z, point) => {
                // extract year from acq_date column
                val alertDate = point.getUserData.asInstanceOf[String].split("\t")(2)
                z.merge(GfwProDashboardDataDateCount.fill(Some(alertDate), 1, viirs = true))

              }
            }

          })
      }
      .reduceByKey(_ merge _)
  }
}
