package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ForestFluxModelPlantedForestType(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "gfw_forest_flux_planted_forest_type"

  val uri: String =
    uriForGrid(gridTile, kwargs)


  //  // For carbon_sensitivity run only (but not currently functional)
  //  val datasetName = "Na"
  //
  //  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  ////  val uri: String =
  ////      s"s3://gfw-data-lake/gfw_planted_forests/v20230911/raster/epsg-4326/{grid_size}/{row_count}/simpleName/geotiff/{tile_id}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1 => "Oil palm"
    case 2 => "Wood fiber"
    case 3 => "Other"
    case _ => "Unknown"
  }
}
