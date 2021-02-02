package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

// FIXME verify path name
case class ProdesLossYear(gridTile: GridTile)
    extends IntegerLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/prodes_loss_year/v1/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/gdal-geotiff/${gridTile.tileId}.tif"
  // FIXME verify lookup values
  override def lookup(value: Int): Integer =
    if (value == 0) value else value + 2000
}
