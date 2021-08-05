package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ProtectedAreas(gridTile: GridTile, kwargs: Map[String, Any]) extends StringLayer with OptionalILayer {

  val datasetName = "wdpa_protected_areas"
  val uri: String = s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/iucn_cat/gdal-geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): String = value match {
    case 1 => "Category Ia/b or II"
    case 2 => "Other Category"
    case _ => ""
  }
}
