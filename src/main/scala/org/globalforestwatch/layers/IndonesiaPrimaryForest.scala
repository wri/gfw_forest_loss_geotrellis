package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IndonesiaPrimaryForest(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
  // TODO
  val uri: String = s"$basePath/idn_primary_forest_2000/v20140601/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
