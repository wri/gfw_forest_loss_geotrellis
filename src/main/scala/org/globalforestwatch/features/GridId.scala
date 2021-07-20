package org.globalforestwatch.features

case class GridId(gridId: String) extends FeatureId {
  override def toString: String = gridId
}
