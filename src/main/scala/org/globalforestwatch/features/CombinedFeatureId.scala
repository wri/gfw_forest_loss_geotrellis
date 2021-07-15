package org.globalforestwatch.features

case class CombinedFeatureId(featureId1: FeatureId, featureId2: FeatureId) extends FeatureId {
  override def toString: String = s"$featureId1" + s"$featureId2"
}