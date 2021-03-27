package org.globalforestwatch.features

import cats.data.NonEmptyList
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.util.Util.getAnyMapValue

object FireAlertRDD {

  def apply(spark: SparkSession,
            kwargs: Map[String, Any]): SpatialRDD[Geometry] = {
    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val fireAlertUris
    : NonEmptyList[String] = getAnyMapValue[Option[NonEmptyList[String]]](
      kwargs,
      "fireAlertSource"
    ) match {
      case None =>
        throw new java.lang.IllegalAccessException(
          "fire_alert_source parameter required for fire alerts analysis"
        )
      case Some(s: NonEmptyList[String]) => s
    }
    val fireAlertObj =
      FeatureFactory("firealerts", Some(fireAlertType)).featureObj
    val fireAlertPointDF = SpatialFeatureDF(
      fireAlertUris,
      fireAlertObj,
      kwargs,
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
