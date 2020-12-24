package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IntactPrimaryForest(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {

  val uri: String =
//    s"$basePath/gfw_intact_or_primary_forest_2000/v20180628/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/year/gdal-geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/2018_update/ifl_primary/standard/${gridTile.tileId}.tif"
}
