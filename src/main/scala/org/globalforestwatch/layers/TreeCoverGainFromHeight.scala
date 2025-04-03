package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

// This is for tree_cover_gain_from_height, which is just a boolean for gain from 2000-2020.
case class TreeCoverGainFromHeight(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "umd_tree_cover_gain_from_height"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
