package org.globalforestwatch.layers

case class Landmark(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/landmark/v20191111/$grid.tif"
}
