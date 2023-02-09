package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Mangroves1996(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "gmw_global_mangrove_extent_1996"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
