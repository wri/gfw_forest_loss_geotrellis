package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridId.toGladGridId
import org.globalforestwatch.grids.GridTile


case class IntegratedAlerts(gridTile: GridTile, kwargs: Map[String, Any]) extends DateConfLayer with OptionalILayer {
  val datasetName = "gfw_integrated_alerts"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}
