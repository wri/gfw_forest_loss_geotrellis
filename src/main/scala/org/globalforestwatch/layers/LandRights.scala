package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class LandRights(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "gfw_land_rights"
  val uri: String =
    uriForGrid(gridTile)
}
