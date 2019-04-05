package org.globalforestwatch.layers

class WoodFiber(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/wood_fiber/$grid.tif"
}
