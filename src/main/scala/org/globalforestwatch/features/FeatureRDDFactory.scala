package org.globalforestwatch.features

import cats.data.NonEmptyList
import geotrellis.vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.util.Util._

object FeatureRDDFactory {
  def apply(analysis: String,
            featureType: String,
            featureUris: NonEmptyList[String],
            kwargs: Map[String, Any],
            spark: SparkSession): RDD[vector.Feature[vector.Geometry, FeatureId]] = {
    val featureObj = FeatureFactory(featureType).featureObj

    analysis match {
      case "firealerts" =>
        val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
        val fireSrcUris: NonEmptyList[String] = getAnyMapValue[Option[NonEmptyList[String]]](kwargs, "fireAlertSource") match {
          case None => throw new java.lang.IllegalAccessException("fire_alert_source parameter required for fire alerts analysis")
          case Some(s: NonEmptyList[String]) => s
        }

        val fireFeatureObj = FeatureFactory("firealerts", Some(fireAlertType)).featureObj
        PointInPolygonFeatureRDD(
          featureUris,
          featureObj,
          fireSrcUris,
          fireFeatureObj,
          kwargs,
          spark).distinct  // call distinct to remove duplicate PIP intersections due to how we split input geometries
      case _ =>
        FeatureRDD(featureUris, featureObj, kwargs, spark)
    }
  }
}
