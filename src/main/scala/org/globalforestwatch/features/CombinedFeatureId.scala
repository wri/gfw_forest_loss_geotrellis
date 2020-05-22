package org.globalforestwatch.features

case class CombinedFeatureId(feature1: FeatureId, feature2: FeatureId) extends FeatureId {
  override def toString: String = feature1.toString() + " " + feature2.toString
}