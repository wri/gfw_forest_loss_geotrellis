package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossAnnualAbovegroundRemovalsCarbon(gridTile: GridTile,
                                                model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "Na"
  override lazy val version = "Na"

  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  val uri: String =
  //    s"$basePath/gfw_gross_annual_aboveground_removals_carbon$model_suffix/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg_ha-1/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/flux_1_2_1/annual_removal_factor_AGC_all_forest_types/$model_suffix/${gridTile.tileId}.tif"
}
