package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IndonesiaForestMoratorium(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {

  val uri: String = s"$basePath/idn_forest_moratorium/v20190123/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
