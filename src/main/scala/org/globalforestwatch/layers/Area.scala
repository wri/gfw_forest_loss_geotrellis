package org.globalforestwatch.layers

case class Area(grid: String) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"$basePath/area/$grid.tif"
}
