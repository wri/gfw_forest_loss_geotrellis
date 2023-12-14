package org.globalforestwatch.raster

import geotrellis.raster.Tile

case object SBTNNaturalForests extends RasterLayer {
  val name = "sbtn_natural_forests_map"

  type OUT = Option[String]
  def convert(t: Tile, col: Int, row: Int): Option[String] = t.get(col, row) match {
    case 0 => Some("Non-Forest")
    case 1 => Some("Natural Forest")
    case 2 => Some("Non-Natural Forest")
    case _ => None
  }
}
