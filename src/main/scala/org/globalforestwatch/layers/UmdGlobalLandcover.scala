package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class UmdGlobalLandcover(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {
  val datasetName = "umd_land_cover"
  val uri: String =
    uriForGrid(gridTile, kwargs)

  override def lookup(value: Int): String = {
    value match {
      case v if v >= 0 && v <= 24 => "Grassland"
      case v if (v >= 25 && v <= 96) || (v >= 125 && v <= 196) => "Forest"
      case v if v >= 100 && v <= 124 => "Wetlands"
      case v if v >= 244 && v <= 247 => "Cropland"
      case v if v >= 250 && v <= 253 => "Built-Up Area"
      case v if (v >= 200 && v <= 215) || (v >= 241 && v <= 243) || v == 254 => "Other"
      case _ => "Unknown"
    }
  }
}
