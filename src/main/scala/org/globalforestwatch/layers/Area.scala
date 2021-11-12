package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Area(gridTile: GridTile, kwargs: Map[String, Any]) extends DoubleLayer with RequiredDLayer {

  val datasetName = "gfw_pixel_area"

  val uri: String = uriForGrid(gridTile)
}
