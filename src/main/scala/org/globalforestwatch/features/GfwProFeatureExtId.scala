package org.globalforestwatch.features

// GfwPro FeatureId that includes commodity and yield for GHG analysis.

case class GfwProFeatureExtId(listId: String, locationId: Int, commodity: String, yieldVal: Float) extends FeatureId {
  override def toString: String = s"$listId, $locationId, $commodity, $yieldVal"
}
