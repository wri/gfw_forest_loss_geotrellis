package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class LandRights(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/gfw_land_rights/v2016/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
