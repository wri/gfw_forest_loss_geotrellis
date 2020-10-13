package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Erosion(gridTile: GridTile) extends StringLayer with OptionalILayer {
  val uri: String = s"$basePath/aqueduct_erosion_risk/latest/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/level/gdal-geotiff/${gridTile.tileId}.tif"
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1 => "Low risk"
    case 2 => "Low to medium risk"
    case 3 => "Medium to high risk"
    case 4 => "High risk"
    case 5 => "Extremely high risk"
    case _ => "Unknown"
  }
}