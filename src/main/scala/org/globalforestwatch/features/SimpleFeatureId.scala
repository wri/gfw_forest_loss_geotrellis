package org.globalforestwatch.features

case class SimpleFeatureId(cell_id: Int) {
  override def toString: String = s"$cell_id"
}
