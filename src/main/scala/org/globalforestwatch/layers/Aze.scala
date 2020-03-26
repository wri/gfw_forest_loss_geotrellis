package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

import org.globalforestwatch.grids.{Grid, GridSources, GridTile}

case class Aze(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/birdlife_alliance_for_zero_extinction_site/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
