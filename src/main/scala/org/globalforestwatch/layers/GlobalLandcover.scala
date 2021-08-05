package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GlobalLandcover(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "esa_land_cover_2015"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/class/gdal-geotiff/${gridTile.tileId}.tif"
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 10 | 11 | 12 | 20 | 30 | 40 => "Agriculture"
    case 50 | 60 | 61 | 62 | 70 | 71 | 72 | 80 | 81 | 82 | 90 | 100 | 160 |
         170 => "Forest"
    case 110 | 130 => "Grassland"
    case 180 => "Wetland"
    case 190                         => "Settlement"
    case 120 | 121 | 122             => "Shrubland"
    case 140 | 150 | 151 | 152 | 153 => "Sparse vegetation"
    case 200 | 201 | 202             => "Bare"
    case 210                         => "Water"
    case 220                         => "Permanent snow and ice"
    case _ => "Unknown"

  }
}
