package org.globalforestwatch.features

case class ModisFireAlertFeatureId(
                                    lon: Double,
                                    lat: Double,
                                  alertDate: String,
                                  alertTime: Int,
                                  confidence: Int,
                                  brightness: Float,
                                  brightT31: Float,
                                  frp: Float) extends FeatureId {
  override def toString: String = alertDate + " " + alertTime.toString
}
