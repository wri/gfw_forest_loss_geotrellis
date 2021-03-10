package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class GrossEmissionsNodeCodes(gridTile: GridTile, model: String = "standard")
  extends StringLayer
    with OptionalILayer {
  val model_suffix: String = if (model == "standard") "standard" else s"$model"

  val uri: String =
//    s"$basePath/gfw_emissions_node_codes$model_suffix/v20200824/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/name/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/flux_1_2_1/gross_emissions_node_codes/$model_suffix/${gridTile.tileId}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 10 => "Commodity, peat, burned"
    case 11 => "Commodity, peat, not burned, tropical, plantation"
    case 111 => "Commodity, peat, not burned, tropical, not plantation"
    case 12 => "Commodity, peat, not burned, temperate/boreal"
    case 13 => "Commodity, not peat, burned, tropical, IFL, plantation"
    case 131 => "Commodity, not peat, burned, tropical, IFL, not plantation"
    case 14 => "Commodity, not peat, burned, tropical, not IFL, plantation"
    case 141 => "Commodity, not peat, burned, tropical, not IFL, not plantation"
    case 15 => "Commodity, not peat, burned, boreal"
    case 16 => "Commodity, not peat, burned, temperate, plantation"
    case 161 => "Commodity, not peat, burned, temperate, not plantation"
    case 17 => "Commodity, not peat, not burned, tropical, plantation"
    case 171 => "Commodity, not peat, not burned, tropical, not plantation"
    case 18 => "Commodity, not peat, not burned, boreal"
    case 19 => "Commodity, not peat, not burned, temperate, plantation"
    case 191 => "Commodity, not peat, not burned, temperate, not plantation"

    case 20 => "Shifting ag, peat, burned, temperate/boreal"
    case 21 => "Shifting ag, peat, burned, tropical"
    case 22 => "Shifting ag, peat, not burned, temperate/boreal"
    case 23 => "Shifting ag, peat, not burned, tropical, plantation"
    case 231 => "Shifting ag, peat, not burned, tropical, not plantation"
    case 24 => "Shifting ag, not peat, burned, tropical, IFL, plantation"
    case 241 => "Shifting ag, not peat, burned, tropical, IFL, not plantation"
    case 25 => "Shifting ag, not peat, burned, tropical, not IFL, plantation"
    case 251 => "Shifting ag, not peat, burned, tropical, not IFL, not plantation"
    case 26 => "Shifting ag, not peat, burned, boreal"
    case 27 => "Shifting ag, not peat, burned, temperate, plantation"
    case 271 => "Shifting ag, not peat, burned, temperate, not plantation"
    case 28 => "Shifting ag, not peat, not burned, tropical, plantation"
    case 281 => "Shifting ag, not peat, not burned, tropical, not plantation"
    case 29 => "Shifting ag, not peat, not burned, boreal"
    case 291 => "Shifting ag, not peat, not burned, temperate, plantation"
    case 292 => "Shifting ag, not peat, not burned, temperate, not plantation"

    case 30 => "Forestry, peat, burned"
    case 31 => "Forestry, peat, not burned, temperate/boreal"
    case 32 => "Forestry, peat, not burned, tropical, plantation"
    case 321 => "Forestry, peat, not burned, tropical, not plantation"
    case 33 => "Forestry, not peat, burned"
    case 34 => "Forestry, not peat, not burned"

    case 40 => "Wildfire, peat, burned"
    case 41 => "Wildfire, peat, not burned, temperate/boreal"
    case 42 => "Wildfire, peat, not burned, tropical, plantation"
    case 421 => "Wildfire, peat, not burned, tropical, not plantation"
    case 43 => "Wildfire, not peat, burned"
    case 44 => "Wildfire, not peat, not burned"

    case 50 => "Urbanization, peat, burned"
    case 51 => "Urbanization, peat, not burned, tropical, plantation"
    case 511 => "Urbanization, peat, not burned, tropical, not plantation"
    case 52 => "Urbanization, peat, not burned, temperate/boreal"
    case 53 => "Urbanization, not peat, burned, tropical, IFL, plantation"
    case 531 => "Urbanization, not peat, burned, tropical, IFL, not plantation"
    case 54 => "Urbanization, not peat, burned, tropical, not IFL, plantation"
    case 541 => "Urbanization, not peat, burned, tropical, not IFL, not plantation"
    case 55 => "Urbanization, not peat, burned, boreal"
    case 56 => "Urbanization, not peat, burned, temperate, plantation"
    case 561 => "Urbanization, not peat, burned, temperate, not plantation"
    case 57 => "Urbanization, not peat, not burned, tropical, plantation"
    case 571 => "Urbanization, not peat, not burned, tropical, not plantation"
    case 58 => "Urbanization, not peat, not burned, boreal"
    case 59 => "Urbanization, not peat, not burned, temperate, plantation"
    case 591 => "Urbanization, not peat, not burned, temperate, not plantation"

    case 60 => "No driver, peat, burned"
    case 61 => "No driver, peat, not burned, temperate/boreal"
    case 62 => "No driver, peat, not burned, tropical, plantation"
    case 621 => "No driver, peat, not burned, tropical, not plantation"
    case 63 => "No driver, not peat, burned"
    case 64 => "No driver, not peat, not burned"
    case _ => "Unknown"
  }
}
