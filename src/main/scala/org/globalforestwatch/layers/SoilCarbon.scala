package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SoilCarbon(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with OptionalILayer {

  val datasetName = "gfw_soil_carbon"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
