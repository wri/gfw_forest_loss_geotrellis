package org.globalforestwatch.features

case class FireAlertFeatureId(alertDate: String, acqTime: Int, confidence: String, brightTi4: Float, brightTi5: Float, frp: Float, feature: FeatureId) extends FeatureId {
  override def toString: String = feature.toString()
}