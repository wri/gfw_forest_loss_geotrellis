package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

case class SEAsiaLandCover(gridTile: GridTile)
    extends StringLayer
    with OptionalILayer {

  val uri: String = uriForGrid(GfwConfig.get.rasterLayers(getClass.getSimpleName()), gridTile)

  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1 | 3 | 4 => "Primary forest"
    case 2 | 5 | 6 => "Secondary forest"
    case 7 => "Rubber plantation"
    case 8 => "Oil palm plantation"
    case 9 => "Timber plantation"
    case 10 => "Mixed tree crops"
    case 11 | 15 => "Grassland/shrub"
    case 12 | 16 => "Swamp"
    case 13 | 17 => "Agriculture"
    case 14 => "Settlements"
    case 18 => "Coastal fish pond"
    case 19 => "Bare land"
    case 20 => "Mining"
    case 21 => "Water bodies"
    case 22 => "No data"
    case _ => "Unknown"
  }
}
