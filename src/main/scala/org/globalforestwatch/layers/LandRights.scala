package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class LandRights(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "gfw_land_rights"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
