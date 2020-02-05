package org.globalforestwatch.features

case class FireAlertFeatureId(alertDate: String, feature: FeatureId) extends FeatureId {
  override def toString: String = feature.toString()
}