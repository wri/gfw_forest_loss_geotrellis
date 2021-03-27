package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.{
  Geometry => GeoSparkGeometry,
  Point => GeoSparkPoint
}
import geotrellis.vector
import geotrellis.vector.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.globalforestwatch.util.GeometryReducer

object FeatureRDD {
  def apply(
             input: NonEmptyList[String],
             featureObj: Feature,
             kwargs: Map[String, Any],
             spark: SparkSession
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {
    val featuresDF: DataFrame =
      FeatureDF(input, featureObj, kwargs, spark)

    featuresDF.rdd.mapPartitions({ iter: Iterator[Row] =>
      for {
        i <- iter
        if featureObj.isValidGeom(i)
      } yield {
        featureObj.get(i)
      }
    }, preservesPartitioning = true)
  }

  def apply(
             featureObj: Feature,
             spatialRDD: SpatialRDD[GeoSparkGeometry],
             kwargs: Map[String, Any]
           ): RDD[geotrellis.vector.Feature[Geometry, FeatureId]] = {

    val scalaRDD =
      org.apache.spark.api.java.JavaRDD.toRDD(spatialRDD.spatialPartitionedRDD)

    scalaRDD.map {
      case pt: GeoSparkPoint =>
        val pointFeatureData = pt.getUserData.asInstanceOf[String].split('\t')

        val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
          vector.Point(pt.getX, pt.getY)
        )

        val pointFeatureId: FeatureId =
          featureObj.getFeatureId(pointFeatureData)
        vector.Feature(geom, pointFeatureId)
      case _ => throw new NotImplementedError("Cannot convert geometry type to Geotrellis RDD")
    }

  }
}
