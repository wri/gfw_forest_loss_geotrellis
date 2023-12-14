package org.globalforestwatch.raster

import geotrellis.raster.Tile

case object GadmAdm2 extends RasterLayer {
  val name: String = "gadm_adm2"

  type OUT = Option[Int]
  def convert(t: Tile, col: Int, row: Int): Option[Int] = {
    val value = t.get(col, row)
    if (value == 9999) None else Some(value)
  }
}
