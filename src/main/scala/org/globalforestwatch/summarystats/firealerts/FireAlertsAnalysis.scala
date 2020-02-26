package org.globalforestwatch.summarystats.firealerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.Feature
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{FeatureDF, FeatureFactory, FeatureId, FireAlertFeature, ViirsFireAlertFeatureId}
import org.globalforestwatch.summarystats.firealerts.FireAlertsRDD.SUMMARY
import org.globalforestwatch.util.Util._
import org.datasyslab.geospark.spatialRDD.{PointRDD, SpatialRDD}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import java.util.HashSet

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.{Geometry, Point, Polygon}

import collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.spark.api.java.JavaRDD
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter

object FireAlertsAnalysis {
  def apply(featureObj: org.globalforestwatch.features.Feature,
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val fireAlertFeatureRDD = getFireAlertFeatureRDDGeoSpark(spark, featureObj, kwargs)

    val inputPartitionMultiplier = 64
    val part = new HashPartitioner(
      partitions = fireAlertFeatureRDD.getNumPartitions * inputPartitionMultiplier
    )

    val summaryRDD: RDD[(FeatureId, FireAlertsSummary)] =
      FireAlertsRDD(fireAlertFeatureRDD, FireAlertsGrid.blockTileGrid, part, kwargs)

    val summaryDF =
      FireAlertsDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame

    summaryDF.repartition(partitionExprs = $"featureId")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/fireAlerts_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    FireAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }

  def getFireAlertFeatureRDDGeoSpark(spark: SparkSession,
                                     featureObj: org.globalforestwatch.features.Feature,
                             kwargs: Map[String, Any])(implicit kt: ClassTag[SUMMARY], vt: ClassTag[FeatureId], ord: Ordering[SUMMARY] = null): RDD[Feature[geotrellis.vector.Geometry, FeatureId]]  = {
    val fireSrcUris: NonEmptyList[String] = getAnyMapValue[Option[NonEmptyList[String]]](kwargs, "fireAlertSource") match {
      case None => throw new java.lang.IllegalAccessException("fire_alert_source parameter required for fire alerts analysis")
      case Some(s: NonEmptyList[String]) => s
    }

    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")

    val featureUris = getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris")

    val fireAlertDF = FeatureDF(fireSrcUris, FeatureFactory("firealerts").featureObj, kwargs, spark, "longitude", "latitude")
    var fireAlertRDD = new PointRDD
    fireAlertRDD.rawSpatialRDD = Adapter.toJavaRdd(fireAlertDF).asInstanceOf[JavaRDD[Point]]

    val featureDF = FeatureDF(featureUris, featureObj, kwargs, spark, "geom")
    var featureRDD = new SpatialRDD[Geometry]
    featureRDD.rawSpatialRDD = Adapter.toJavaRdd(featureDF)

    val considerBoundaryIntersection = false // Only return geometries fully covered by each query window in queryWindowRDD
    fireAlertRDD.analyze()

    fireAlertRDD.spatialPartitioning(GridType.QUADTREE)
    featureRDD.spatialPartitioning(fireAlertRDD.getPartitioner)

    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    featureRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

    val joinResultRDD: org.apache.spark.api.java.JavaPairRDD[Geometry, HashSet[Point]] = JoinQuery.SpatialJoinQuery(fireAlertRDD, featureRDD, true, considerBoundaryIntersection)
    val joinResultScalaRDD: RDD[(Geometry, HashSet[Point])] = org.apache.spark.api.java.JavaPairRDD.toRDD(joinResultRDD)

    joinResultScalaRDD.flatMap {
      case (geom: Geometry, points: HashSet[Point]) =>
        val geomFields = geom.getUserData.asInstanceOf[String].split('\t')
        val featureId = featureObj.getFeatureId(geomFields)

        points.asScala.map((pt: Point) => {
          val fireFeatureData = pt.getUserData.asInstanceOf[String].split('\t')
          FireAlertFeature.getFireAlertFeature(fireAlertType, pt.getX, pt.getY, fireFeatureData, featureId)
        })
    }
  }
}
