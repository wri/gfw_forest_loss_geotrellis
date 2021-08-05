package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PeruProtectedAreas(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "per_protected_areas"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
