package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.summarystats.firealerts.FireAlertsAnalysis.pairRddToDf

object PolygonIntersectionDF {
  /*
    Applies a spatial join between two polygonal datasets using GeoSpark, returning the
    intersecting polygons with combined attributes.

    NOTE: The spatial join will partition/index on feature 1, so typically feature 1 should
    be the larger dataset.
   */
  def apply(feature1Uris: NonEmptyList[String],
            feature1Type: String,
            feature2Uris: NonEmptyList[String],
            feature2Type: String,
            spark: SparkSession,
            feature1Filters: FeatureFilter,
            feature2Filters: FeatureFilter,
            feature1Delimiter: String = "\t",
            feature2Delimiter: String = "\t"): DataFrame = {

    val feature1DF: DataFrame =
      SpatialFeatureDF(feature1Uris, feature1Type, feature1Filters, "geom", spark, feature1Delimiter)

    val feature2DF: DataFrame =
      SpatialFeatureDF(feature2Uris, feature2Type, feature2Filters, "geom", spark, feature2Delimiter)

    PolygonIntersectionDF(feature1DF, feature2DF, spark)
  }


  def apply(feature1DF: DataFrame, feature2DF: DataFrame, spark: SparkSession): DataFrame = {
    feature1DF.createOrReplaceTempView("left")
    feature2DF.createOrReplaceTempView("right")
    spark.sql(
      "SELECT " +
        "left.featureId as featureId1, " +
        "right.featureId as featureId2, " +
        "ST_Intersection(left.polyshape, right.polyshape) as intersectedshape " +
        "FROM left, right " +
        "WHERE ST_Intersects(left.polyshape, right.polyshape)")
  }
}
