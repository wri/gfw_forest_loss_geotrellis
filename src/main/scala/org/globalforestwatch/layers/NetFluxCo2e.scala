package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class NetFluxCo2e(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {
  val datasetName = "gfw_full_extent_net_flux"

  val uri: String =
    uriForGrid(gridTile)
}
