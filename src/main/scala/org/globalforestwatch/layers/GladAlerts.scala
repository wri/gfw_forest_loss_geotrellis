package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

import java.time.LocalDate
import org.globalforestwatch.grids.GridId.toGladGridId


case class GladAlerts(gridTile: GridTile, kwargs: Map[String, Any]) extends DateConfLayer with OptionalILayer {
  val datasetName = "umd_glad_landsat_alerts"
  val gladGrid: String = toGladGridId(gridTile.tileId)

  val uri: String =
    uriForGrid(gridTile, kwargs)
}
