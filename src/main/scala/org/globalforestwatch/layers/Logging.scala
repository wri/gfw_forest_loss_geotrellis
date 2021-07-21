package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Logging(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "gfw_managed_forests"
  val uri: String =
    s"$basePath/$datasetName/$version/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
