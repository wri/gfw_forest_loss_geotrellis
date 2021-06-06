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

    val feature1DF: DataFrame =
      SpatialFeatureDF(feature1Uris, feature1Obj, kwargs, "geom", spark, feature1Delimiter).as(feature1Type)

    val feature2DF: DataFrame =
      SpatialFeatureDF(feature2Uris, feature2Obj, kwargs, "geom", spark, feature2Delimiter).as(feature2Type)

    PolygonIntersectionRDD(feature1DF, feature2DF, spark, kwargs)
  }


  def apply(feature1DF: DataFrame,
            feature2DF: DataFrame,
            spark: SparkSession,
            kwargs: Map[String, Any]): SpatialRDD[Geometry] = {

    //    val feature1Type = "left"
    //    val feature2Type = "right"
    //
    //    val joinedDF = feature1DF.alias(feature1Type)
    //      .join(feature2DF.alias(feature2Type))
    //      .where(s"ST_Intersects(${feature1Type}.polyshape, ${feature2Type}.polyshape)")
    //      .withColumn("intersectedshape", expr(s"ST_Intersection(${feature1Type}.polyshape, ${feature2Type}.polyshape)"))
    //      .select(col(s"${feature1Type}.featureId") as "featureId1", col(s"${feature2Type}.featureId") as "featureId2", col("intersectedshape"))

    feature1DF.createOrReplaceTempView("left")
    feature2DF.createOrReplaceTempView("right")

    val joinedDF = spark.sql(
      "SELECT " +
        "left.featureId as featureId1, " +
        "right.featureId as featureId2, " +
        "ST_Intersection(left.polyshape, right.polyshape) as intersectedshape " +
        "FROM left, right " +
        "WHERE ST_Intersects(left.polyshape, right.polyshape)")



    //    joinedDF.show(5)

    //    val joinedDF = feature1DF.alias("left")
    //      .join(feature2DF.alias("right"))
    //      .where(s"ST_Intersects(left.polyshape, right.polyshape)")
    //      .withColumn("intersectedshape", expr(s"ST_Intersection(left.polyshape, right.polyshape)"))
    //      .select(col("left.featureId") as "featureId1", col("right.featureId") as "featureId2", col("intersectedshape"))

    Adapter.toSpatialRdd(joinedDF, "intersectedshape")
  }
}
