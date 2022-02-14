package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile


case class Mining(gridTile: GridTile, kwargs: Map[String, Any]) extends BooleanLayer with OptionalILayer {
  val datasetName = "gfw_mining_concessions"
  val uri: String =
    uriForGrid(gridTile)
}
