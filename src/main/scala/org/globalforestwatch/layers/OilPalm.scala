package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class OilPalm(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "gfw_oil_palm"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
