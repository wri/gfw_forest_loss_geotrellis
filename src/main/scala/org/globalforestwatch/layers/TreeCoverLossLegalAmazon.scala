package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLossLegalAmazon(gridTile: GridTile) extends IntegerLayer with OptionalILayer {
  val uri: String = s"$basePath/legal_Amazon_annual_loss/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/geotiff/${gridTile.tileId}.tif"

  override def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
