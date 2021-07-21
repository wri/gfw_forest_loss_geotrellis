package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SEAsiaLandCover(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "rspo_southeast_asia_land_cover_2010"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/land_cover_class/gdal-geotiff/${gridTile.tileId}.tif"

  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1 | 3 | 4 => "Primary forest"
    case 2 | 5 | 6 => "Secondary forest"
    case 7 => "Rubber plantation"
    case 8 => "Oil palm plantation"
    case 9 => "Timber plantation"
    case 10 => "Mixed tree crops"
    case 11 | 15 => "Grassland/ shrub"
    case 12 | 16 => "Swamp"
    case 13 | 17 => "Agriculture"
    case 14 => "Settlements"
    case 18 => "Coastal fish pond"
    case 19 => "Bare land"
    case 20 => "Mining"
    case 21 => "Water bodies"
    case 22 => "No data"
    case _ => "Unknown"
  }
}
