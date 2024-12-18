package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ForestFluxModelGrossEmissionsNodeCodes(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "gfw_forest_flux_gross_emissions_node_codes"

  val uri: String =
    uriForGrid(gridTile, kwargs)

  //  // For carbon_sensitivity run only (but not currently functional)
  //  val datasetName = "Na"
  //
  //  val model_suffix: String = if (model == "standard") "standard" else s"$model"
  //  val uri: String =
  //      s"s3://gfw-data-lake/gfw_forest_flux_gross_emissions_node_codes/v20231114/raster/epsg-4326/{grid_size}/{row_count}/category/geotiff/{tile_id}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {

    case 10=> "Permanent agriculture, peat, burned"
    case 11=> "Permanent agriculture, peat, not burned, tropical, plantation"
    case 111=> "Permanent agriculture, peat, not burned, tropical, not plantation"
    case 12=> "Permanent agriculture, peat, not burned, temperate/boreal"
    case 13=> "Permanent agriculture, not peat, burned, tropical, IFL, plantation"
    case 131=> "Permanent agriculture, not peat, burned, tropical, IFL, not plantation"
    case 14=> "Permanent agriculture, not peat, burned, tropical, not IFL, plantation"
    case 141=> "Permanent agriculture, not peat, burned, tropical, not IFL, not plantation"
    case 15=> "Permanent agriculture, not peat, burned, boreal"
    case 16=> "Permanent agriculture, not peat, burned, temperate, plantation"
    case 161=> "Permanent agriculture, not peat, burned, temperate, not plantation"
    case 17=> "Permanent agriculture, not peat, not burned, tropical, plantation"
    case 171=> "Permanent agriculture, not peat, not burned, tropical, not plantation"
    case 18=> "Permanent agriculture, not peat, not burned, boreal"
    case 19=> "Permanent agriculture, not peat, not burned, temperate, plantation"
    case 191=> "Permanent agriculture, not peat, not burned, temperate, not plantation"

    case 20=> "Hard commodities, peat, burned"
    case 21=> "Hard commodities, peat, not burned, tropical, plantation"
    case 211=> "Hard commodities, peat, not burned, tropical, not plantation"
    case 22=> "Hard commodities, peat, not burned, temperate/boreal"
    case 23=> "Hard commodities, not peat, burned, tropical, IFL, plantation"
    case 231=> "Hard commodities, not peat, burned, tropical, IFL, not plantation"
    case 24=> "Hard commodities, not peat, burned, tropical, not IFL, plantation"
    case 241=> "Hard commodities, not peat, burned, tropical, not IFL, not plantation"
    case 25=> "Hard commodities, not peat, burned, boreal"
    case 26=> "Hard commodities, not peat, burned, temperate, plantation"
    case 261=> "Hard commodities, not peat, burned, temperate, not plantation"
    case 27=> "Hard commodities, not peat, not burned, tropical, plantation"
    case 271=> "Hard commodities, not peat, not burned, tropical, not plantation"
    case 28=> "Hard commodities, not peat, not burned, boreal"
    case 29=> "Hard commodities, not peat, not burned, temperate, plantation"
    case 291=> "Hard commodities, not peat, not burned, temperate, not plantation"

    case 30=> "Shifting cultivation, peat, burned, temperate/boreal"
    case 31=> "Shifting cultivation, peat, burned, tropical"
    case 32=> "Shifting cultivation, peat, not burned, temperate/boreal"
    case 33=> "Shifting cultivation, peat, not burned, tropical, plantation"
    case 331=> "Shifting cultivation, peat, not burned, tropical, not plantation"
    case 34=> "Shifting cultivation, not peat, burned, tropical, IFL, plantation"
    case 341=> "Shifting cultivation, not peat, burned, tropical, IFL, not plantation"
    case 35=> "Shifting cultivation, not peat, burned, tropical, not IFL, plantation"
    case 351=> "Shifting cultivation, not peat, burned, tropical, not IFL, not plantation"
    case 36=> "Shifting cultivation, not peat, burned, boreal"
    case 37=> "Shifting cultivation, not peat, burned, temperate, plantation"
    case 371=> "Shifting cultivation, not peat, burned, temperate, not plantation"
    case 38=> "Shifting cultivation, not peat, not burned, tropical, plantation"
    case 381=> "Shifting cultivation, not peat, not burned, tropical, not plantation"
    case 39=> "Shifting cultivation, not peat, not burned, boreal"
    case 391=> "Shifting cultivation, not peat, not burned, temperate, plantation"
    case 392=> "Shifting cultivation, not peat, not burned, temperate, not plantation"

    case 40=> "Forest management, peat, burned"
    case 41=> "Forest management, peat, not burned, temperate/boreal"
    case 42=> "Forest management, peat, not burned, tropical, plantation"
    case 421=> "Forest management, peat, not burned, tropical, not plantation"
    case 43=> "Forest management, not peat, burned"
    case 44=> "Forest management, not peat, not burned"

    case 50=> "Wildfire, peat, burned"
    case 51=> "Wildfire, peat, not burned, temperate/boreal"
    case 52=> "Wildfire, peat, not burned, tropical, plantation"
    case 521=> "Wildfire, peat, not burned, tropical, not plantation"
    case 53=> "Wildfire, not peat, burned"
    case 54=> "Wildfire, not peat, not burned"

    case 60=> "Settlements & infrastructure, peat, burned"
    case 61=> "Settlements & infrastructure, peat, not burned, tropical, plantation"
    case 611=> "Settlements & infrastructure, peat, not burned, tropical, not plantation"
    case 62=> "Settlements & infrastructure, peat, not burned, temperate/boreal"
    case 63=> "Settlements & infrastructure, not peat, burned, tropical, IFL, plantation"
    case 631=> "Settlements & infrastructure, not peat, burned, tropical, IFL, not plantation"
    case 64=> "Settlements & infrastructure, not peat, burned, tropical, not IFL, plantation"
    case 641=> "Settlements & infrastructure, not peat, burned, tropical, not IFL, not plantation"
    case 65=> "Settlements & infrastructure, not peat, burned, boreal"
    case 66=> "Settlements & infrastructure, not peat, burned, temperate, plantation"
    case 661=> "Settlements & infrastructure, not peat, burned, temperate, not plantation"
    case 67=> "Settlements & infrastructure, not peat, not burned, tropical, plantation"
    case 671=> "Settlements & infrastructure, not peat, not burned, tropical, not plantation"
    case 68=> "Settlements & infrastructure, not peat, not burned, boreal"
    case 69=> "Settlements & infrastructure, not peat, not burned, temperate, plantation"
    case 691=> "Settlements & infrastructure, not peat, not burned, temperate, not plantation"

    case 70=> "Other natural disturbances, peat, burned"
    case 71=> "Other natural disturbances, peat, not burned, temperate/boreal"
    case 72=> "Other natural disturbances, peat, not burned, tropical, plantation"
    case 721=> "Other natural disturbances, peat, not burned, tropical, not plantation"
    case 73=> "Other natural disturbances, not peat, burned"
    case 74=> "Other natural disturbances, not peat, not burned"

    case 80=> "No driver, peat, burned"
    case 81=> "No driver, peat, not burned, temperate/boreal"
    case 82=> "No driver, peat, not burned, tropical, plantation"
    case 821=> "No driver, peat, not burned, tropical, not plantation"
    case 83=> "No driver, not peat, burned"
    case 84=> "No driver, not peat, not burned"

    case _ => "Unknown"
  }
}
