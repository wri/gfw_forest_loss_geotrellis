package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverGain(gridTile: GridTile) extends BooleanLayer with RequiredILayer {
  val uri: String = s"$basePath/umd_tree_cover_gain/v1.6/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"

  override val internalNoDataValue: Int = 0
  override val externalNoDataValue: Boolean = false

  override def lookup(value: Int): Boolean = !(value == 0)
}
