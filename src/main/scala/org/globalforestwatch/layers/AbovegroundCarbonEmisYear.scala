package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class AbovegroundCarbonEmisYear(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_forest_flux_aboveground_carbon_stock_in_emissions_year"

  val uri: String =
    uriForGrid(gridTile, kwargs)

  //  // For carbon_sensitivity run only (but not currently functional)
  //  val datasetName = "Na"

  //  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  //  val uri: String =
  //    s"s3://gfw-data-lake/gfw_forest_flux_aboveground_carbon_stock_in_emissions_year/v20231114/raster/epsg-4326/{grid_size}/{row_count}/Mg_C_ha-1/geotiff/{tile_id}.tif"
}
