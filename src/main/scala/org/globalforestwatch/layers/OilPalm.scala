package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class OilPalm(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/gfw_oil_palm/v20191031/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
