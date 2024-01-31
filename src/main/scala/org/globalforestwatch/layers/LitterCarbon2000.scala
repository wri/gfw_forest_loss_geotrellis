package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class LitterCarbon2000(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_litter_carbon_stock_2000"

  val uri: String =
    uriForGrid(gridTile, kwargs)


//  // For carbon_sensitivity run only (but not currently functional)
//  val datasetName = "Na"
//
//  val model_suffix: String = if (model == "standard") "standard" else s"$model"
//  val uri: String =
//    s"s3://gfw-data-lake/gfw_litter_carbon_stock_2000/v20230222/raster/epsg-4326/{grid_size}/{row_count}/Mg_C_ha-1/geotiff/{tile_id}.tif"
}
