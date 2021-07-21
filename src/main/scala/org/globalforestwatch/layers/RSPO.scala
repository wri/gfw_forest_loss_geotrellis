package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class RSPO(gridTile: GridTile, kwargs: Map[String, Any]) extends StringLayer with OptionalILayer {

  val datasetName = "rspo_oil_palm"
  val uri: String = s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/certification_status/gdal-geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): String = value match {
    case 1 => "Certified"
    case 2 => "Unknown"
    case 3 => "Not certified"
    case _ => ""
  }
}
