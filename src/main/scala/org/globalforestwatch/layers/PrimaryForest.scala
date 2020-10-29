package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PrimaryForest(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String =  s"$basePath/umd_regional_primary_forest_2001/v201901/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
