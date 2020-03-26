package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SoilCarbon2000(gridTile: GridTile, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/soil_carbon_2000/$model/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
