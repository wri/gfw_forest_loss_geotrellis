package org.globalforestwatch.layers

case class Landmark(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/landmark/$grid.tif"
}
