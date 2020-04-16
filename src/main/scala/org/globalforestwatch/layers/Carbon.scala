package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Carbon(gridTile: GridTile) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"$basePath/co2_pixel/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
