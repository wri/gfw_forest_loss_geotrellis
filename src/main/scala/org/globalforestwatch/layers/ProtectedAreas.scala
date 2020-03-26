package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ProtectedAreas(gridTile: GridTile) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/wdpa_protected_areas/v201912/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/iucn_cat/geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): String = value match {
    case 1 => "Category Ia/b or II"
    case 2 => "Other Category"
    case _ => ""
  }
}
