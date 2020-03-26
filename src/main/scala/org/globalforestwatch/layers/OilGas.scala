package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class OilGas(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/gfw_oil_gas/v20190321/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
