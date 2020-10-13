package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TigerLandscapes(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/gfw_tiger_landscapes/v201904/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
