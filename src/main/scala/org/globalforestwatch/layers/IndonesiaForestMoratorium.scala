package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IndonesiaForestMoratorium(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {

  val uri: String = s"$basePath/idn_forest_moratorium/v20200923/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
