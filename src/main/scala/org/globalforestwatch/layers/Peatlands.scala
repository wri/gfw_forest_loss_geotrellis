package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Peatlands(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

    val datasetName = "gfw_peatlands"

    val uri: String =
      uriForGrid(gridTile, kwargs)
}
