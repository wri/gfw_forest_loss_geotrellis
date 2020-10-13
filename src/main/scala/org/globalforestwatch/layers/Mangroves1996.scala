package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Mangroves1996(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/gmw_mangroves_1996/v20180701/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
