package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Plantations(gridTile: GridTile) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/gfw_plantations/v1.3/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/type/geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): String = value match {
    case 1  => "Fruit"
    case 2  => "Fruit Mix"
    case 3  => "Oil Palm "
    case 4  => "Oil Palm Mix"
    case 5  => "Other"
    case 6  => "Rubber"
    case 7  => "Rubber Mix"
    case 8  => "Unknown"
    case 9  => "Unknown Mix"
    case 10 => "Wood fiber / Timber"
    case 11 => "Wood fiber / Timber Mix"
    case _ => ""
  }
}

case class PlantationsBool(gridTile: GridTile) extends BooleanLayer with OptionalILayer {

  val uri: String = s"$basePath/gfw_plantations/v1.3/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/type/geotiff/${gridTile.tileId}.tif"

}