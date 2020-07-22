package org.globalforestwatch.summarystats.firealerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{CombinedFeatureId, FeatureDF, FeatureFactory, FeatureId, FireAlertModisFeatureId, FireAlertViirsFeatureId, GadmFeatureId, GeostoreFeatureId}
import org.globalforestwatch.summarystats.firealerts.FireAlertsRDD.SUMMARY
import org.globalforestwatch.util.Util._
import org.datasyslab.geospark.spatialRDD.{PointRDD, SpatialRDD}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import java.util.HashSet

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.{Geometry, Point}
import geotrellis.vector

import collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.spark.api.java.JavaRDD
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.util.GeometryReducer

object FireAlertsAnalysis {
  def apply(featureRDD: RDD[Feature[vector.Geometry, FeatureId]],
            featureType: String,
            part: HashPartitioner,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val layoutDefinition = fireAlertType match {
      case "viirs" => ViirsGrid.blockTileGrid
      case "modis" => ModisGrid.blockTileGrid
    }

    val summaryRDD: RDD[(FeatureId, FireAlertsSummary)] =
      FireAlertsRDD(featureRDD, layoutDefinition, part, kwargs)

    val fireDF = summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case CombinedFeatureId(viirsId: FireAlertViirsFeatureId, gadmId: GadmFeatureId) =>
                  FireAlertsRowViirs(viirsId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a valid Fire Alert ID")
              }
            }
          }
      }
      .toDF("fireId", "data_group", "data")

    val fireViewName = "fire_alerts"
    fireDF.createOrReplaceTempView(fireViewName)
    val firePointDF = spark.sql(
      s"""
         |SELECT ST_Point(CAST(fireId.lon AS Decimal(24,10)),CAST(fireId.lat AS Decimal(24,10))) AS pointshape, *
         |FROM $fireViewName
      """.stripMargin)

    firePointDF.createOrReplaceTempView(fireViewName)

    var pointFeatureRDD = new PointRDD
    pointFeatureRDD.rawSpatialRDD = Adapter.toJavaRdd(firePointDF).asInstanceOf[JavaRDD[Point]]

    val fireFeatureObj = FeatureFactory("firealerts", Some(fireAlertType)).featureObj
    val featureObj = FeatureFactory(featureType).featureObj
    val featureUris: NonEmptyList[String] = getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris") // match {
//      case None => throw new java.lang.IllegalAccessException("feature_uri parameter required for analysis")
//      case Some(s: NonEmptyList[String]) => s
//    }

    val polyFeatureDF = FeatureDF(featureUris, featureObj, kwargs, spark)
    val featureViewName = featureObj.getClass.getSimpleName.dropRight(1).toLowerCase

    val polySpatialDf = spark.sql(
      s"""
         |SELECT ST_PrecisionReduce(ST_GeomFromWKB(geom), 13) AS polyshape, *
         |FROM $featureViewName
         |WHERE geom != '0106000020E610000000000000'
      """.stripMargin)



    val joined = spark.sql(
      s"""
         |SELECT $featureViewName.*, $fireViewName.*
         |FROM $fireViewName, $featureViewName
         |WHERE ST_Intersects($featureViewName.polyshape, $fireViewName.pointshape)
      """.stripMargin
    )

    joined.show()



    val summaryDF =
      FireAlertsDFFactory(featureType, summaryRDD, spark, kwargs).getDataFrame

    summaryDF.repartition(partitionExprs = $"featureId")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      s"/firealerts_${fireAlertType}_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    FireAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }
}
