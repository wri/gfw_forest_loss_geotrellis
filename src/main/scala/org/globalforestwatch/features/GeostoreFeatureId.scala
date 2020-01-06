package org.globalforestwatch.features

case class GeostoreFeatureId(geostoreId: String) extends FeatureId {
  override def toString: String = s"$geostoreId"
}
