package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridId.toGladGridId
import org.globalforestwatch.grids.GridTile

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class RaddAlerts(gridTile: GridTile, kwargs: Map[String, Any])
  extends DateConfLayer
    with OptionalILayer {
  val datasetName = "wur_radd_alerts"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/date_conf/gdal-geotiff/${gridTile.tileId}.tif"
}
