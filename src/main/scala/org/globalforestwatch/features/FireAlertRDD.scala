package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.SparkSession
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter


object FireAlertRDD {
  def apply(
    spark: SparkSession,
    fireAlertType: String,
    fireAlertSource: NonEmptyList[String],
    filters: FeatureFilter
  ): SpatialRDD[Geometry] = {
    val fireAlertObj = Feature(fireAlertType)
    val fireAlertPointDF = SpatialFeatureDF(
      fireAlertSource,
      fireAlertObj,
      filters,
      spark,
      "longitude",
      "latitude"
    )

    val fireAlertSpatialRDD =
      Adapter.toSpatialRdd(fireAlertPointDF, "pointshape")

    fireAlertSpatialRDD.analyze()
    fireAlertSpatialRDD
  }
}
