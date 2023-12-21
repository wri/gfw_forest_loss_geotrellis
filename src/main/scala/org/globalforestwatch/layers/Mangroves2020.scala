package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Mangroves2020(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "gmw_global_mangrove_extent_2020"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
