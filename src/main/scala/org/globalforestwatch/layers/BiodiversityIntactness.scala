package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class BiodiversityIntactness(gridTile: GridTile) extends DBooleanLayer with OptionalDLayer {
    val uri: String = s"$basePath/birdlife_biodiversity_intactness/v201909/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/index/geotiff/${gridTile.tileId}.tif"

  def lookup(value: Double): Boolean =
  // if (value == 0) null
    if (value > 2.515) true
    //    else if (value > 2.39) 90
    //    else if (value > -2.26) 80
    //    else if (value > -11.97) 70
    //    else if (value > -12.31) 60
    //    else if (value > -22.31) 50
    //    else if (value > -29.09) 40
    //    else if (value > -31.19) 30
    //    else if (value > -33.43) 20
    //    else if (value > -100) 10
    else false

}