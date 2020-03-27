package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class LitterCarbonEmisYear(gridTile: GridTile, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val model_suffix = if (model == "standard") "" else s"__{$model}"
  val uri: String = s"$basePath/gfw_litter_carbon_stock_in_emissions_year$model_suffix/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg/geotiff/${gridTile.tileId}.tif"
}
