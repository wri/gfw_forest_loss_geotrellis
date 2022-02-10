package org.globalforestwatch.features

case class GfwProFeatureId(listId: String, locationId: Int, x: Double, y: Double) extends FeatureId {
  override def toString: String = s"$listId, $locationId, $x, $y"
}
