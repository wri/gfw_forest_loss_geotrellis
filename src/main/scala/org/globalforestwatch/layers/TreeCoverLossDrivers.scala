package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLossDrivers(gridTile: GridTile)
    extends StringLayer
    with OptionalILayer {
  val uri: String = s"$basePath/tsc_tree_cover_loss_drivers/v2019/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/type/geotiff/${gridTile.tileId}.tif"

  override val internalNoDataValue = 0
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 2 => "Commodity driven deforestation"
    case 3 => "Shifting agriculture"
    case 4 => "Forestry"
    case 5 => "Wildfire"
    case 6 => "Urbanization"
    case _ => "Unknown"
  }
}
