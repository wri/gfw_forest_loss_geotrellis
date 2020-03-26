package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Bgc2000(gridTile: GridTile, model: String = "standard") extends FloatLayer with OptionalFLayer {
  val uri: String = s"$basePath/bgc_2000/$model/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
