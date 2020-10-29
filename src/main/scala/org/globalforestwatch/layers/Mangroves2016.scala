package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Mangroves2016(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/gmw_mangroves_2016/v20180701/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
