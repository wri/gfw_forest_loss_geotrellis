package org.globalforestwatch.layers

case class Peatlands(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/peatlands/$grid.tif"
}
