package org.globalforestwatch.layers

case class WoodFiber(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/wood_fiber/v20190917/ $grid.tif"
}
