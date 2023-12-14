package org.globalforestwatch.raster

import geotrellis.raster.Tile

case object NegligibleRisk extends RasterLayer {

  val name = "gfwpro_negligible_risk_analysis"

  type OUT = String
  def convert(t: Tile, col: Int, row: Int): String = t.get(col, row) match {
    case 1 => "NO"
    case 2 => "YES"
    case 3 => "NA"
    case _ => "Unknown"
  }
}
