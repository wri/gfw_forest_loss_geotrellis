package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

import java.time.LocalDate
import org.globalforestwatch.grids.GridId.toGladGridId

case class GladAlerts(gridTile: GridTile) extends DateConfLayer with OptionalILayer {
  val baseDate = LocalDate.of(2014,12,31)
  val gladGrid: String = toGladGridId(gridTile.tileId)

  val uri: String =
    s"s3://gfw2-data/forest_change/umd_landsat_alerts/prod/analysis/$gladGrid.tif"

  override def lookup(value: Int): Option[(LocalDate, Boolean)] = {

    val confidence = value >= 30000
    val days: Int = if (confidence) value - 30000 else value - 20000
    if (days < 0) {
        None
    } else {
        val date = baseDate.plusDays(days)
        Some((date, confidence))
    }
  }
}
