package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.util.Util.getAnyMapValue
import org.apache.spark.sql.functions.{col, expr}

object PolygonIntersectionRDD {
  def apply(feature1Uris: NonEmptyList[String],
            feature1Type: String,
            feature2Uris: NonEmptyList[String],
            feature2Type: String,
            spark: SparkSession,
            kwargs: Map[String, Any],
            feature1Delimiter: String = "\t",
            feature2Delimiter: String = "\t"): SpatialRDD[Geometry] = {

    val feature1DF: DataFrame =
      SpatialFeatureDF(feature1Uris, feature1Type, kwargs, "geom", spark, feature1Delimiter)

    val feature2DF: DataFrame =
      SpatialFeatureDF(feature2Uris, feature2Type, kwargs, "geom", spark, feature2Delimiter)

    PolygonIntersectionRDD(feature1DF, feature2DF, spark, kwargs)
  }


  def apply(feature1DF: DataFrame,
            feature2DF: DataFrame,
            spark: SparkSession,
            kwargs: Map[String, Any]): SpatialRDD[Geometry] = {

    feature1DF.createOrReplaceTempView("left")
    feature2DF.createOrReplaceTempView("right")

    val joinedDF = spark.sql(
      "SELECT " +
        "left.featureId as featureId1, " +
        "right.featureId as featureId2, " +
        "ST_Intersection(left.polyshape, right.polyshape) as intersectedshape " +
        "FROM left, right " +
        "WHERE ST_Intersects(left.polyshape, right.polyshape)")

    Adapter.toSpatialRdd(joinedDF, "intersectedshape")
  }
}
