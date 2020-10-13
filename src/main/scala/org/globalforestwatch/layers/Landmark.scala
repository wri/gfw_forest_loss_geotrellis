package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Landmark(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/landmark_land_rights/v20191111/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
