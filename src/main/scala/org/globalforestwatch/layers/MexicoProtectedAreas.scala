package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class MexicoProtectedAreas(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/mex_protected_areas/v20161003/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
