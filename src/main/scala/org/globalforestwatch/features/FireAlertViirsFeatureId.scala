package org.globalforestwatch.features

case class FireAlertViirsFeatureId(
                                    lon: Double,
                                    lat: Double,
                                    alertDate: String,
                                    alertTime: Int,
                                    confidence: String,
                                    brightTi4: Double,
                                    brightTi5: Double,
                                    frp: Double) extends FeatureId {
  override def toString: String = alertDate + " " + alertTime.toString + " " + lon.toString + " " + lat.toString
}
