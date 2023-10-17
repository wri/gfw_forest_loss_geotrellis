package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class BelowgroundCarbon2000(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_belowground_carbon"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}