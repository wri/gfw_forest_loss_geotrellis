package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class FluxModelExtent(gridTile: GridTile, model: String = "standard")
  extends BooleanLayer
    with OptionalILayer {
  //    val model_suffix = if (model == "standard") s"__standard" else s"__$model"
  val model_suffix = s"__$model"
  val uri: String =
    s"$basePath/gfw_flux_model_extent/v20191031/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
