package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class MapspamYield(commodity: String, gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = s"mapspam_yield_${commodity.toLowerCase()}"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}
