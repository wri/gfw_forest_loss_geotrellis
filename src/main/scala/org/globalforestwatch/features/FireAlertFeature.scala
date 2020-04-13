package org.globalforestwatch.features

import geotrellis.vector
import org.globalforestwatch.util.GeometryReducer
import geotrellis.vector.Geometry
import org.apache.spark.sql.Row

object FireAlertFeature extends Feature {
  def create(fireAlertType: String, x: Double, y: Double, i: Array[String], featureId: FeatureId): vector.Feature[Geometry, FeatureId] = {
    val adjustedX =
      if (x.toString.split("[.]")(1).size == 1)
        x - 0.00001
      else x

    val adjustedY =
      if (y.toString.split("[.]")(1).size == 1)
        y + 0.00001
      else y

    val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
      vector.Point(adjustedX, adjustedY)
    )

    val fireFeatureId = fireAlertType match  {
      case "viirs" =>
        val acqDate: String = i(2)
        val acqTime: Int = i(3).toInt
        val confidence: String = i(4)
        val brightTi4: Float = i(5).toFloat
        val brightTi5: Float = i(6).toFloat
        val frp: Float = i(7).toFloat

        ViirsFireAlertFeatureId(x, y, acqDate, acqTime, confidence, brightTi4, brightTi5, frp)
      case "modis" =>
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

        ModisFireAlertFeatureId(x, y, acqDate, acqTime, confidencePerc, confidenceCat, brightness, brightT31, frp)
    }

    vector.Feature(geom, CombinedFeatureId(fireFeatureId, featureId))
  }

  override val geomPos: Int = 0

  override def get(i: Row): vector.Feature[Geometry, FeatureId] = {
    create("viirs", 0, 0, Array[String](), new EmptyFeatureId)
  }

  override def getFeatureId(i: Array[String]): FeatureId = {
    new EmptyFeatureId
  }
}