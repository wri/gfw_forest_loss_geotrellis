package org.globalforestwatch.features

case class FireAlertModisFeatureId(
                                    lon: Double,
                                    lat: Double,
                                    alertDate: String,
                                    alertTime: Int,
                                    confidencePerc: Int,
                                    confidenceCat: String,
                                    brightness: Float,
                                    brightT31: Float,
                                    frp: Float) extends FeatureId {
  override def toString: String = alertDate + " " + alertTime.toString + " " + lon.toString + " " + lat.toString
}
