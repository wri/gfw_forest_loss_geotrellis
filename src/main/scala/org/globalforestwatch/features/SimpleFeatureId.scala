package org.globalforestwatch.features

case class SimpleFeatureId(featureId: Long) extends FeatureId {
  override def toString: String = s"$featureId"
}
