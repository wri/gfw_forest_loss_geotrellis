package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IntactForestLandscapes(gridTile: GridTile)
  extends StringLayer
    with OptionalILayer {
  val uri: String = s"$basePath/ifl_intact_forest_landscapes/v20180628/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/gdal-geotiff/${gridTile.tileId}.tif"

  def lookup(value: Int): String = value match {
    case 0 => ""
    case _ => value.toString

  }
}

case class IntactForestLandscapes2016(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/ifl_intact_forest_landscapes/v20180628/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/gdal-geotiff/${gridTile.tileId}.tif"

  override def lookup(value: Int): Boolean = {
    value match {
      case 2016 => true
      case _ => false
    }
  }

}
