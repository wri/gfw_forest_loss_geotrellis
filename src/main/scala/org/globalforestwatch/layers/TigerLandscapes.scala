package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TigerLandscapes(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "wwf_tiger_conservation_landscapes"
  val uri: String =
    uriForGrid(gridTile)
}
