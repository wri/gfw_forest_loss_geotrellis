package org.globalforestwatch.features

import java.util.HashSet

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.{Geometry, Point, Polygon}
import geotrellis.vector
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.{PointRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.features.GadmFeature.geomPos
import geotrellis.vector.io.wkb.WKB
import org.globalforestwatch.util.GeometryReducer
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
        // get fire alerts and partition using GeoSpark quadtree
        // this makes the geotrellis analysis significantly faster
        val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
        val fireSrcUris: NonEmptyList[String] = getAnyMapValue[Option[NonEmptyList[String]]](kwargs, "fireAlertSource") match {
          case None => throw new java.lang.IllegalAccessException("fire_alert_source parameter required for fire alerts analysis")
          case Some(s: NonEmptyList[String]) => s
        }

        val fireFeatureObj = FeatureFactory("firealerts", Some(fireAlertType)).featureObj

        val pointFeatureDF = FeatureDF(fireSrcUris, fireFeatureObj, kwargs, spark, "longitude", "latitude")
        var pointFeatureRDD = new PointRDD
        pointFeatureRDD.rawSpatialRDD = Adapter.toJavaRdd(pointFeatureDF).asInstanceOf[JavaRDD[Point]]

        pointFeatureRDD.analyze()
        pointFeatureRDD.spatialPartitioning(GridType.QUADTREE)

        // convert to an RDD usable by geotrellis
        val scalaRDD = org.apache.spark.api.java.JavaRDD.toRDD(pointFeatureRDD.spatialPartitionedRDD)
        scalaRDD.map {
          case pt: Point =>
            val pointFeatureData = pt.getUserData.asInstanceOf[String].split('\t')

            val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
              vector.Point(pt.getX, pt.getY)
            )

            val pointFeatureId: FeatureId = fireFeatureObj.getFeatureId(pointFeatureData)
            vector.Feature(geom, pointFeatureId)
        }
      case _ =>
        FeatureRDD(featureUris, featureObj, kwargs, spark)
    }
  }
}
