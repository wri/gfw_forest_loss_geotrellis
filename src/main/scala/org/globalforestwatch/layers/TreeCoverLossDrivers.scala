package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLossDrivers(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "v2020"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/drivers/gdal-geotiff/${gridTile.tileId}.tif"

  override val internalNoDataValue = 0
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1 => "Commodity driven deforestation"
    case 2 => "Shifting agriculture"
    case 3 => "Forestry"
    case 4 => "Wildfire"
    case 5 => "Urbanization"
    case _ => "Unknown"
  }
}
