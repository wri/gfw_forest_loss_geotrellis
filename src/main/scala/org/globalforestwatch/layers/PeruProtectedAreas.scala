package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PeruProtectedAreas(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/per_protected_areas/v20160901/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
