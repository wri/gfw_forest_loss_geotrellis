package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ProdesLossYear(gridTile: GridTile)
    extends IntegerLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/inpe_prodes/v202106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/gdal-geotiff/${gridTile.tileId}.tif"

  override def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
