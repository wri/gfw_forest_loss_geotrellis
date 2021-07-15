package org.globalforestwatch.features

case class GridFeatureId(featureId: FeatureId, gridId: String) extends FeatureId {
  override def toString: String = s"$featureId - $gridId"
}
