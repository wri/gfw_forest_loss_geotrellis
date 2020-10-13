package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Mining(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/gfw_mining/v20190205/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
