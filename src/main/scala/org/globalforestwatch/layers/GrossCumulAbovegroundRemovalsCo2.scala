package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossCumulAbovegroundRemovalsCo2(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_full_extent_aboveground_gross_removals"

  val uri: String =
    uriForGrid(gridTile)
}
