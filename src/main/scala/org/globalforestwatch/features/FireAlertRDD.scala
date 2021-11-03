package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.summarystats.SummaryCommand


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
