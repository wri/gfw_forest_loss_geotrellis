package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class FluxModelExtent(gridTile: GridTile, model: String = "standard")
  extends BooleanLayer
    with OptionalILayer
{
  //      val model_suffix = if (model == "standard") "" else s"__$model"
  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  val uri: String =
//    s"$basePath/gfw_flux_model_extent$model_suffix/v20191031/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/flux_1_2_1/model_extent/$model_suffix/${gridTile.tileId}.tif"
}
