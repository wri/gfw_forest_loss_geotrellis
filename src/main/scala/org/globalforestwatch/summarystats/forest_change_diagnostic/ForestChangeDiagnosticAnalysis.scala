package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.Logger
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geosparksql.utils.Adapter
//import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
//import org.apache.sedona.core.spatialOperator.JoinQuery
//import org.apache.sedona.core.spatialRDD.PointRDD
//import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{
  FeatureDF,
  FeatureFactory,
  FeatureId,
  SimpleFeatureId
}
import org.globalforestwatch.util.Util.getAnyMapValue

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

object ForestChangeDiagnosticAnalysis {

  val logger = Logger.getLogger("ForestChangeDiagnosticAnalysis")

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {


    val summaryRDD: RDD[(FeatureId, ForestChangeDiagnosticSummary)] =
      ForestChangeDiagnosticRDD(
        featureRDD,
        ForestChangeDiagnosticGrid.blockTileGrid,
        kwargs
      )

    val summaryDF =
      ForestChangeDiagnosticDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/forest_change_diagnostic_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    ForestChangeDiagnosticExport.export(
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
               ): RDD[(SimpleFeatureId, ForestChangeDiagnosticDataLossYearly)] = {

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
            val fireCount =
              points.asScala.toList.foldLeft(SortedMap[Int, Double]()) {
                (z: SortedMap[Int, Double], point) => {
                  // extract year from acq_date column
                  val year = point.getUserData
                    .asInstanceOf[String]
                    .split("\t")(2)
                    .substring(0, 4)
                    .toInt
                  val count = z.getOrElse(year, 0.0) + 1.0
                  z.updated(year, count)
                }
              }
            //TODO make own data class for fires using int and starting in 2000?
            ForestChangeDiagnosticDataLossYearly.prefilled
              .merge(ForestChangeDiagnosticDataLossYearly(fireCount))
          })
      }
      .reduceByKey(_ merge _)
      .mapValues {
        case fires =>
          val minLossYear = fires.value.keysIterator.min
          val maxLossYear = fires.value.keysIterator.max
          val years: List[Int] = List.range(minLossYear, maxLossYear + 1)

          ForestChangeDiagnosticDataLossYearly(
            SortedMap(
              years.map(year => (year, { // I hence declare them here explicitly to help him out.
                val thisYearLoss: Double = fires.value.getOrElse(year, 0)

                val lastYearLoss: Double = fires.value.getOrElse(year - 1, 0)

                thisYearLoss + lastYearLoss / 2
              })): _*))

      }

  }
}
