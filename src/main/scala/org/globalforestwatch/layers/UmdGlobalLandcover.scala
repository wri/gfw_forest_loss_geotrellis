package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class UmdGlobalLandcover(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with OptionalILayer {
  val datasetName = "umd_land_cover"
  val uri: String =
    uriForGrid(gridTile)
}
