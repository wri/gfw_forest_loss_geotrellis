package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossCumulAbovegroundRemovalsCo2(gridTile: GridTile, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  val uri: String =
//    s"$basePath/gfw_gross_cumul_aboveground_removals_co2$model_suffix/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg_ha-1/geotiff/${gridTile.tileId}.tif"
//    s"s3://gfw-files/flux_1_2_1/gross_removals_AGCO2_all_forest_types/$model_suffix/${gridTile.tileId}.tif"
    s"s3://gfw-files/flux_1_2_0/gross_removals_AGCO2_all_forest_types/$model_suffix/${gridTile.tileId}.tif"

}
