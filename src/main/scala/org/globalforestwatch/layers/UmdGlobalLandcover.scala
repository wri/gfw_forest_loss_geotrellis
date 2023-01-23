package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class UmdGlobalLandcover(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with OptionalILayer {
  val datasetName = "umd_land_cover"
  val uri: String =
    uriForGrid(gridTile)

  override def lookup(value: Int): Integer = {
    value match {
      case v if v > 0 && v <= 24 => 1
      case v if v > 24 && v <= 41 => 2
      case v if v > 41 && v <= 48 => 3
      case v if v >= 100 && v <= 124 => 4
      case v if v > 124 && v <= 148 => 5
      case v if v >= 202 && v <= 207 => 6
      case _ => 0
    }
  }
}
