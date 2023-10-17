package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverGain(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "umd_tree_cover_gain"
  val uri: String =
    uriForGrid(gridTile, kwargs)

  override val internalNoDataValue: Int = 0
  override val externalNoDataValue: Boolean = false

  override def lookup(value: Int): Boolean = !(value == 0)
}
