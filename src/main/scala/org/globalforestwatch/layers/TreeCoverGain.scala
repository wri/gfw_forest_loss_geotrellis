package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverGain(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with RequiredILayer {
  val datasetName = "umd_tree_cover_gain"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"

  override val internalNoDataValue: Int = 0
  override val externalNoDataValue: Boolean = false

  override def lookup(value: Int): Boolean = !(value == 0)
}
