package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreesInMosaicLandscapes(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with OptionalILayer {
  val datasetName = "wri_trees_in_mosaic_landscapes"
  val uri: String =
    uriForGrid(gridTile)

  override def lookup(value: Int): Integer = {
    value match {
      case v if v < 10 => 0
      case v if v < 20 => 10
      case v if v < 30 => 20
      case v if v < 40 => 30
      case v if v < 50 => 40
      case v if v < 60 => 50
      case v if v < 70 => 60
      case v if v < 80 => 70
      case v if v < 90 => 80
      case v if v <= 100 => 90
    }
  }
}
