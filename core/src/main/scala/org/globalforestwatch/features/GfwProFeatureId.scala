package org.globalforestwatch.features

case class GfwProFeatureId(listId: String, locationId: Int) extends FeatureId {
  override def toString: String = s"$listId, $locationId"
}
