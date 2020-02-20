package org.globalforestwatch.features

import geotrellis.vector
import org.globalforestwatch.util.GeometryReducer
import geotrellis.vector.{Feature, Geometry, Point}
import org.apache.spark.sql.Row
import org.glassfish.jersey.server.monitoring.RequestEvent.ExceptionCause

object FireAlertFeature extends Feature {
  def getFireAlertFeature(x: Double, y: Double, i: Array[String], featureId: FeatureId) = {
    var adjustedX = x
    var adjustedY = y

    if (x.toString.split("[.]")(1).size == 1) {
      adjustedX = x - 0.00001
    }

    if (y.toString.split("[.]")(1).size == 1) {
      adjustedY = y + 0.00001
    }

    val geom = GeometryReducer.reduce(GeometryReducer.gpr)(
      geotrellis.vector.Point(adjustedX, adjustedY)
    )

    val acqDatePos = 2
    val acqTimePos = 3
    val confidencePos = 4
    val brightTi4Pos = 5
    val brightTi5Pos = 6
    val frpPos = 7

    val acqDate: String = i(acqDatePos)
    val acqTime: Int = i(acqTimePos).toInt
    val confidence: String = i(confidencePos)
    val brightTi4: Float = i(brightTi4Pos).toFloat
    val brightTi5: Float = i(brightTi5Pos).toFloat
    val frp: Float = i(frpPos).toFloat

    val fireFeatureId = FireAlertFeatureId(acqDate, acqTime, confidence, brightTi4, brightTi5, frp, featureId)

    Feature(geom, fireFeatureId)
  }

  override val geomPos: Int = 0

  override def get(i: Row): vector.Feature[Geometry, FeatureId] = {
    getFireAlertFeature(0, 0, Array[String](), new FeatureId)
  }

  override def getFeatureId(i: Array[String]): FeatureId = {
    new FeatureId
  }
}