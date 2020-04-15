package org.globalforestwatch.features

import geotrellis.vector
import geotrellis.vector.Geometry
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object FireAlertsModisFeature extends Feature {
  override val geomPos: Int = 0

  override def get(i: Row): vector.Feature[Geometry, FeatureId] = {
    val featureId = getFeatureId(i)
    val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
      vector.Point(i.getDouble(geomPos), i.getDouble(geomPos + 1))
    )

    geotrellis.vector.Feature(geom, featureId)
  }

  override def getFeatureId(i: Array[String]): FeatureId = {
    val lon: Double = i(0).toDouble
    val lat: Double = i(1).toDouble
    val acqDate: String = i(2)
    val acqTime: Int = i(3).toInt
    val confidencePerc: Int = i(4).toInt
    val confidenceCat: String = confidencePerc match {
      case perc if perc >= 99 => "h"
      case perc if perc >= 40 => "n"
      case _ => "l"
    }
    val brightness: Float = i(5).toFloat
    val brightT31: Float = i(6).toFloat
    val frp: Float = i(7).toFloat

    FireAlertModisFeatureId(lon, lat, acqDate, acqTime, confidencePerc, confidenceCat, brightness, brightT31, frp)
  }
}
