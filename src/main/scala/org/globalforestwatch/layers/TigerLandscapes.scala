package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TigerLandscapes(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "wwf_tiger_conservation_landscapes"
  val uri: String =
    s"$basePath/gfw_tiger_landscapes/v20201207/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
