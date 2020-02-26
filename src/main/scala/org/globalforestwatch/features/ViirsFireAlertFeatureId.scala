package org.globalforestwatch.features

case class ViirsFireAlertFeatureId(
                                    lon: Double,
                                    lat: Double,
                                    alertDate: String,
                                    alertTime: Int,
                                    confidence: String,
                                    brightTi4: Float,
                                    brightTi5: Float,
                                    frp: Float) extends FeatureId {
  override def toString: String = alertDate + " " + alertTime.toString
}
