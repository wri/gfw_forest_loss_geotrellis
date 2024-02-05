package org.globalforestwatch.features

case class AreaFeatureId(featureId1: FeatureId, featureId2: FeatureId, area: Double) extends FeatureId {
  override def toString: String = s"$featureId1\t$featureId2\t$area"
}
