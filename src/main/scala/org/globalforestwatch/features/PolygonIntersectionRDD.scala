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
            feature1Obj: Feature,
            feature1Type: String,
            feature2Uris: NonEmptyList[String],
            feature2Obj: Feature,
            feature2Type: String,
            spark: SparkSession,
            kwargs: Map[String, Any],
            feature1Delimiter: String = "\t",
            feature2Delimiter: String = "\t"): SpatialRDD[Geometry] = {

    val features1DF: DataFrame =
      SpatialFeatureDF(feature1Uris, feature1Obj, kwargs, "geom", spark, feature1Delimiter).as(feature1Type)

    features1DF.show()

    val features2DF: DataFrame =
      SpatialFeatureDF(feature2Uris, feature2Obj, kwargs, "geom", spark, feature2Delimiter).as(feature2Type)

    features2DF.show()
    val joinedDF = features1DF
      .join(features2DF)
      .where(s"ST_Intersects(${feature1Type}.polyshape, ${feature2Type}.polyshape)")
      .withColumn("intersectedshape", expr(s"ST_Intersection(${feature1Type}.polyshape, ${feature2Type}.polyshape)"))
      .select(col(s"${feature1Type}.featureId") as "featureId1", col(s"${feature2Type}.featureId") as "featureId2", col("intersectedshape"))

    joinedDF.show()
    Adapter.toSpatialRdd(joinedDF, "intersectedshape")
  }
}
