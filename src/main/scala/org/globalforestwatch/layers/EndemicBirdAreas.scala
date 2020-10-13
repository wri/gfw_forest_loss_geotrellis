package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class EndemicBirdAreas(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/birdlife_endemic_bird_areas/v2014/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
