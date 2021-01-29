package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLossFirstYear20012015Mekong(gridTile: GridTile) extends IntegerLayer with OptionalILayer {
    val uri: String = s"$basePath/Mekong_first_year_annual_loss_2001_2015/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/geotiff/${gridTile.tileId}.tif"

  override def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
