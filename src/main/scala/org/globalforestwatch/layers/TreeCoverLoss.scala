package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLoss(gridTile: GridTile) extends IntegerLayer with RequiredILayer {
  val uri: String = s"$basePath/umd_tree_cover_loss/v1.8/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/gdal-geotiff/${gridTile.tileId}.tif"

  override def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
