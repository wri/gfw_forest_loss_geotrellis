package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class BrazilBiomes(gridTile: GridTile) extends StringLayer with OptionalILayer {

  val uri: String =
    s"$basePath/bra_biomes/v20150601/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/name/gdal-geotiff/${gridTile.tileId}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1 => "Caatinga"
    case 2 => "Cerrado"
    case 3 => "Pantanal"
    case 4 => "Pampa"
    case 5 => "Amazônia"
    case 6 => "Mata Atlântica"
    case _ => "Unknown"
  }
}
