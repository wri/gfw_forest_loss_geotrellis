package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GFWProPeatlands(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "gfwpro_peatlands"
  val uri: String =
    uriForGrid(gridTile)
}
