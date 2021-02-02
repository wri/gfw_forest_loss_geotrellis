package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SEAsiaLandCover(gridTile: GridTile)
    extends StringLayer
    with OptionalILayer {

  //FIXME: Need to verify data lake path
  val uri: String =
    s"$basePath/south_east_asia_land_cover/v2015/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/name/gdal-geotiff/${gridTile.tileId}.tif"

  override val externalNoDataValue = "Unknown"

  //FIXME: Need to verify legend
  def lookup(value: Int): String = value match {
    case 1 => "Rubber Plantation"
    case 2 => "Secondary Forest"
    case _ => "Unknown"
  }
}
