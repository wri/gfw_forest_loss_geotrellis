package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Peatlands(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/gfw_peatlands/v20190103/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
