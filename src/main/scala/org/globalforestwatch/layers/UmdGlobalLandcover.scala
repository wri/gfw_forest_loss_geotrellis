package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class UmdGlobalLandcover(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {
  val datasetName = "umd_land_cover"
  override val externalNoDataValue = "Other"

  val uri: String =
    uriForGrid(gridTile, kwargs)

  override def lookup(value: Int): String = {
    value match {
      case v if v == 1 => "Grassland"
      case v if v == 2 => "Forest"
      case v if v == 3 => "Wetlands"
      case v if v == 4 => "Cropland"
      case v if v == 5 => "Settlements"
      case v if v == 6 => "Other"
    }
  }
}
