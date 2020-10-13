package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PeatlandsFlux(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/peatlands_flux/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
