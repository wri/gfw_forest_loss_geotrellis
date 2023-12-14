package org.globalforestwatch.raster

import geotrellis.raster.Tile
import org.globalforestwatch.layout.RasterLayerGrid

trait RasterLayer {
  def name: String
  val required: Boolean = false

  def resolve(grid: RasterLayerGrid): RealizedLayer = {
    RealizedLayer(this, grid)
  }

  type OUT
  def convert(t: Tile, col: Int, row: Int): OUT

  // TODO: Add a lookup method to wrap convert to ignore NODATA cells
}
