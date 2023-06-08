package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class RemovalForestType(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "Na"

  val model_suffix: String = if (model == "standard") "standard" else s"$model"

  val uri: String =
      s"s3://gfw-files/flux_1_2_3/removal_forest_type/$model_suffix/${gridTile.tileId}.tif"

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
