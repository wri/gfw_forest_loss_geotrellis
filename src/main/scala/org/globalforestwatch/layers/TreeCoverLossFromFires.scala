package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLossFromFires(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "umd_tree_cover_loss_from_fires"
  val uri: String =
    uriForGrid(gridTile)
}
