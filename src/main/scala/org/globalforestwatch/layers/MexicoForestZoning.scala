package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class MexicoForestZoning(gridTile: GridTile) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/gfw_land_rights/v20160502/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/zone/gdal-geotiff/${gridTile.tileId}.tif"

  override val externalNoDataValue: String = ""

  def lookup(value: Int): String = value match {
    case 1 | 2 | 3 | 4 | 5 | 6 | 7 => "Zonas de conservación y aprovechamiento restringido o prohibido"

    case 8 | 9 | 10 | 11 | 12 | 13 => "Zonas de producción"

    case 14 | 15 | 16 | 17 | 18 => "Zonas de restauración"

    case 19 => "No aplica"

    case _ => ""
  }
}
