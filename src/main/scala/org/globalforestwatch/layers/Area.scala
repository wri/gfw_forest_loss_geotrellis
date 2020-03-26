package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Area(gridTile: GridTile) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"$basePath/gfw_pixel_area/20150327/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/m2/geotiff/${gridTile.tileId}.tif"
}
