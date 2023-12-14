package org.globalforestwatch.raster

import geotrellis.raster.Tile

case object TreeCoverLoss extends RasterLayer {
  val name = "umd_tree_cover_loss"
  override val required = true

  type OUT = Option[Int]
  def convert(t: Tile, col: Int, row:Int): Option[Int] = {
    val value = t.get(col, row)
    if (value == 0) None else Some(value + 2000)
  }
}
