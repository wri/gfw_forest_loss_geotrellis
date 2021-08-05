package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Mining(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/gfw_mining_concessions/v202106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
