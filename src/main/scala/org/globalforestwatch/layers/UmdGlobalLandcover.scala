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
      case v if v <= 50 => 1
      case v if v <= 100 => 2
      case v if v <= 150 => 3
      case _ => 4
    }
  }
}
