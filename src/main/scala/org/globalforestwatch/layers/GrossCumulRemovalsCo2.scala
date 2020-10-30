package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossCumulRemovalsCo2(gridTile: GridTile, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val model_suffix = if (model == "standard") "" else s"__$model"
  val uri: String = s"$basePath/gfw_gross_cumul_removals_co2$model_suffix/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg/gdal-geotiff/${gridTile.tileId}.tif"
}
