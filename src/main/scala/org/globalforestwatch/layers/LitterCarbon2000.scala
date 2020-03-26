package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class LitterCarbon2000(gridTile: GridTile, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/litter_carbon_2000/$model/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
