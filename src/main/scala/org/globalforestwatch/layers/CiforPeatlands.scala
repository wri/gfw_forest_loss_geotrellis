package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class CiforPeatlands(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
  // TODO use final data lake source
  val uri: String =
    s"s3://gfw-data-lake-dev/cifor_peatlands/v2/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
