package org.globalforestwatch.layers

class Landmark(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/landmark/$grid.tif"
}
