package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TotalCarbon2000(gridTile: GridTile)
  extends FloatLayer
    with OptionalFLayer {
  val uri: String =
    s"$basePath/gfw_total_carbon_stock_2000/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg_ha-1/geotiff/${gridTile.tileId}.tif"
}
