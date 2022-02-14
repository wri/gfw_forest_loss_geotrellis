package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Logging(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "gfw_managed_forests"
  val uri: String =
    uriForGrid(gridTile)
}
