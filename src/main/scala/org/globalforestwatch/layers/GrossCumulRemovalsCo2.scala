package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossCumulRemovalsCo2(gridTile: GridTile, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/gross_cumul_removals_co2/$model/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
