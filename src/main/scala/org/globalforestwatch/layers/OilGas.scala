package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class OilGas(gridTile: GridTile, kwargs: Map[String, Any]) extends BooleanLayer with OptionalILayer {
  val datasetName = "gfw_oil_gas"
  val uri: String =
    uriForGrid(gridTile)
}
