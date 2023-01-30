package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SoilCarbon(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_soil_carbon_stocks"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
