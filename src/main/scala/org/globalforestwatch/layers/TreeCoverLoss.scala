package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLoss(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with RequiredILayer {
  val datasetName = "umd_tree_cover_loss"
  val uri: String =
    uriForGrid(gridTile)

  override def lookup(value: Int): Integer =
    if (value == 0) null else value + 2000
}
