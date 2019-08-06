package org.globalforestwatch.features

case class SimpleFeatureId(cell_id: Int) extends FeatureId {
  override def toString: String = s"$cell_id"
}
