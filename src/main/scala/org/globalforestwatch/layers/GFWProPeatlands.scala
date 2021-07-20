package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GFWProPeatlands(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"s3://gfw-data-lake/gfwpro_peatlands/v2019/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
