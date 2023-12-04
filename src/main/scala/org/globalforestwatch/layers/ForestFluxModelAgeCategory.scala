package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ForestFluxModelAgeCategory(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "gfw_forest_flux_forest_age_category"

  val uri: String =
    uriForGrid(gridTile, kwargs)


  //  // For carbon_sensitivity run only (but not currently functional)
  //  val datasetName = "Na"
  //
  //  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  //  val uri: String =
  //    s"s3://gfw-data-lake/gfw_forest_flux_forest_age_category/v20231114/raster/epsg-4326/{grid_size}/{row_count}/ageCategory/geotiff/{tile_id}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1 => "Secondary forest <=20 years"
    case 2  => "Secondary forest >20 years"
    case 3  => "Primary forest"
    case _ => "Unknown"
  }
}
