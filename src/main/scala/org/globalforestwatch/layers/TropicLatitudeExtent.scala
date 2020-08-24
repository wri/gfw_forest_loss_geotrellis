package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TropicLatitudeExtent(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/tropic_latitude_extent/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
