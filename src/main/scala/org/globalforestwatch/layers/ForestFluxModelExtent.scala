package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ForestFluxModelExtent(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "gfw_forest_flux_model_extent"

  val uri: String =
    uriForGrid(gridTile, kwargs)


  //  // For carbon_sensitivity run only (but not currently functional)
  //  val datasetName = "Na"
  //
  //  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  //  val uri: String =
  //      s"s3://gfw-data-lake/gfw_forest_flux_model_extent/v20231114/raster/epsg-4326/{grid_size}/{row_count}/ha/geotiff/{tile_id}.tif"
}
