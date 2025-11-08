package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class DistAlerts(gridTile: GridTile, kwargs: Map[String, Any]) extends DistDateConfLayer with OptionalILayer {
  val datasetName = "umd_glad_dist_alerts"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}
