package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class WoodFiber(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "gfw_wood_fiber"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
