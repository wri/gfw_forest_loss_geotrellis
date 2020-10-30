package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class BiodiversitySignificance(gridTile: GridTile) extends DBooleanLayer with OptionalDLayer {
  val uri: String = s"$basePath/birdlife_biodiversity_significance/v201909/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/index/gdal-geotiff/${gridTile.tileId}.tif"

  def lookup(value: Double): Boolean =
    if (value > 6.3) true
    //    else if (value > 3.03) 90
    //    else if (value > 1.76) 80
    //    else if (value > 1.23) 70
    //    else if (value > 0.676) 60
    //    else if (value > 0.34) 50
    //    else if (value > 0.245) 40
    //    else if (value > 0.191) 30
    //    else if (value > 0.144) 20
    //    else if (value > 0) 10
    else false

}