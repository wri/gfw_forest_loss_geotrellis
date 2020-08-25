package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class FiaRegionsUsExtent(gridTile: GridTile)
  extends StringLayer
    with OptionalILayer {

  val uri: String = s"$basePath/usfs_fia_regions/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/name/geotiff/${gridTile.tileId}.tif"
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1  => "PNWE"
    case 2  => "PNWW"
    case 3  => "PSW"
    case 4  => "RMS"
    case 5  => "NE"
    case 6  => "SE"
    case 7  => "NLS"
    case 8  => "SC"
    case 9  => "CS"
    case 10 => "GP"
    case 11 => "RMN"
    case _ => ""
  }
}
