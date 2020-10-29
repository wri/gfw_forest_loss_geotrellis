package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ResourceRights(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/gfw_resource_rights/v2015/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
