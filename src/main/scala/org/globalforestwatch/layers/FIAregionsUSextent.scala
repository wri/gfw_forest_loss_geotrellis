package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class FiaRegionsUsExtent(gridTile: GridTile) extends StringLayer with OptionalILayer {
  // TODO
  val uri: String = s"$basePath/FIA_regions_US_extent/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1  => "NE"
    case 2  => "NLS"
    case 3  => "NPS"
    case 4  => "PSW"
    case 5  => "PWE"
    case 6  => "PWW"
    case 7  => "RMN"
    case 8  => "RMS"
    case 9  => "SC"
    case 10 => "SE"
    case _ => "Unknown"
  }
}
