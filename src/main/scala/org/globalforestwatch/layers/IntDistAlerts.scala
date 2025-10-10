package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IntDistAlerts(gridTile: GridTile, kwargs: Map[String, Any]) extends DateConfLevelsLayer with OptionalILayer {
  val datasetName = "gfw_integrated_dist_alerts"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}
