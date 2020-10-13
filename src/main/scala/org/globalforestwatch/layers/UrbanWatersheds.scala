package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class UrbanWatersheds(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/tnc_urban_water_intake/v2016/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
