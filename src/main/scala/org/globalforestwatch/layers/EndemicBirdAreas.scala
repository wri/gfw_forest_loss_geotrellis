package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class EndemicBirdAreas(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "birdlife_endemic_bird_areas"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
