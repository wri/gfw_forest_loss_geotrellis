package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ForestFluxModelRemovalForestType(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "gfw_forest_flux_removal_forest_type"

  val uri: String =
    uriForGrid(gridTile, kwargs)


  //  // For carbon_sensitivity run only (but not currently functional)
  //  val datasetName = "Na"
  //
  //  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  ////  val uri: String =
  ////      s"s3://gfw-data-lake/gfw_forest_flux_removal_forest_type/v20231114/v20231114/raster/epsg-4326/{grid_size}/{row_count}/source/geotiff/{tile_id}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1 => "IPCC Table 4.9 default old (>20 year) secondary and primary rates"
    case 2 => "Young (<20 year) natural forest rates (Cook-Patton et al. 2020)"
    case 3 => "US-specific rates (USFS FIA)"
    case 4 => "Planted forest rates"
    case 5 => "European forest rates"
    case 6 => "Mangrove rates (IPCC Wetlands Supplement)"
    case _ => "Unknown"
  }
}
