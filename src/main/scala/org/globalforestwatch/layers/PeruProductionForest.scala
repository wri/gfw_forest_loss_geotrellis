package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PeruProductionForest(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/per_permanent_production_forests/v20150901/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
