package org.globalforestwatch.layers

class Area(grid: String) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"$basePath/area/$grid.tif"
}
