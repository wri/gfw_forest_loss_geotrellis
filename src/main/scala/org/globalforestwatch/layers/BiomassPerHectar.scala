package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class BiomassPerHectar(gridTile: GridTile) extends DoubleLayer with OptionalDLayer {
  val uri: String = s"$basePath/whrc_aboveground_biomass_stock_2000/v4/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg_ha-1/geotiff/${gridTile.tileId}.tif"
}
