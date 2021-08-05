package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Mangroves2016(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "gmw_global_mangrove_extent_2016"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
