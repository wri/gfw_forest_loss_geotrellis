package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class JplAGBextent(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/jpl_AGB_extent/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
