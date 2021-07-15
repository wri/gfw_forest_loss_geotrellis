package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SoyPlantedAreas(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/umd_soy_planted_area/v2019/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/soy_planted_area/geotiff/${gridTile.tileId}.tif"
}
