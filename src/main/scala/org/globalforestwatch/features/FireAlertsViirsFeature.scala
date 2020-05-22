package org.globalforestwatch.features

import geotrellis.vector
import geotrellis.vector.Geometry
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object FireAlertsViirsFeature extends Feature {
  override val geomPos: Int = 0

  override def get(i: Row): vector.Feature[Geometry, FeatureId] = {
    val featureId = getFeatureId(i)
    val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
      vector.Point(i.getDouble(geomPos + 1), i.getDouble(geomPos))
    )

    geotrellis.vector.Feature(geom, featureId)
  }

  override def getFeatureId(i: Array[String]): FeatureId = {
    val lat: Double = i(0).toDouble
    val lon: Double = i(1).toDouble
    val acqDate: String = i(2)
    val acqTime: Int = i(3).toInt
    val confidence: String = i(4)
    val brightTi4: Float = i(5).toFloat
    val brightTi5: Float = i(6).toFloat
    val frp: Float = i(7).toFloat

    FireAlertViirsFeatureId(lon, lat, acqDate, acqTime, confidence, brightTi4, brightTi5, frp)
  }
}
