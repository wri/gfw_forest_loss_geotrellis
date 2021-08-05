package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PeruProductionForest(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/osinfor_peru_permanent_production_forests/v2015/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
