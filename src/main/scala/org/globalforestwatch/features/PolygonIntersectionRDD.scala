package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.util.Util.getAnyMapValue
import org.apache.spark.sql.functions.expr

object PolygonIntersectionRDD {
  def apply(feature1Uris: NonEmptyList[String],
            feature1Obj: Feature,
            feature1Type: String,
            feature2Uris: NonEmptyList[String],
            feature2Obj: Feature,
            feature2Type: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): SpatialRDD[Geometry] = {

    val features1DF: DataFrame =
      SpatialFeatureDF(feature1Uris, feature1Obj, kwargs, spark, "geom").as(feature1Type)

    val features2DF: DataFrame =
      SpatialFeatureDF(feature2Uris, feature2Obj, kwargs, spark, "geom").as(feature2Type)

    val joinedRDD = features1DF
      .join(features2DF)
      .where(s"ST_Intersects(${feature1Type}.polyshape, ${feature2Type}.polyshape)")
      .withColumn("polyshape", expr(s"ST_Intersection(${feature1Type}.polyshape, ${feature2Type}.polyshape)"))
      .select(s"${feature1Type}.featureId, ${feature2Type}.featureId, polyshape")

    Adapter.toSpatialRdd(joinedRDD, "polyshape")
  }
}
