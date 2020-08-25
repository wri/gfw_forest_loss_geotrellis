package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class MangroveBiomassExtent(gridTile: GridTile)
  extends DBooleanLayer
    with OptionalDLayer {

  val uri: String =
    s"$basePath/jpl_mangrove_aboveground_biomass_stock_2000/v20190215/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg_ha-1/geotiff/${gridTile.tileId}.tif"

  def lookup(value: Double): Boolean = {
    if (value == 0) false
    else true
  }

}
