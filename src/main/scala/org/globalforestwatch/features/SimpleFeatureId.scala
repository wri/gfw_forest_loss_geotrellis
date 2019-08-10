package org.globalforestwatch.features

case class SimpleFeatureId(featureId: Int) extends FeatureId {
  override def toString: String = s"$featureId"
}
