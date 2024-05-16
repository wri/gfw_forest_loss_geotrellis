package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile


case class RaddAlerts(gridTile: GridTile, kwargs: Map[String, Any])
  extends DateConfLayer
    with OptionalILayer {
  val datasetName = "wur_radd_alerts"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
