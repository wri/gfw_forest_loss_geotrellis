package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class MangrovesLatest(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "gmw_global_mangrove_extent_latest"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
