package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossAnnualAbovegroundRemovalsCarbon(gridTile: GridTile, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
      val model_suffix = if (model == "standard") "" else s"__$model"
  val uri: String =
//    s"$basePath/gfw_gross_annual_aboveground_removals_carbon$model_suffix/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg_ha-1/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/flux_2_1_0/annual_removal_factor_AGC_all_forest_types/standard/${gridTile.tileId}.tif"
}
